package au.edu.utas.lm_nfs_sg.saas.master;

import au.edu.lm_nf_sg.saas.common.job.JobStatus;
import au.edu.lm_nf_sg.saas.common.worker.WorkerType;
import au.edu.utas.lm_nfs_sg.saas.master.job.Job;
import au.edu.utas.lm_nfs_sg.saas.master.job.SparkJob;
import au.edu.utas.lm_nfs_sg.saas.master.worker.JCloudsNova;
import au.edu.utas.lm_nfs_sg.saas.master.worker.Worker;

import com.google.gson.*;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;

import java.nio.file.Path;
import java.util.*;

/**
 * Created by nico on 25/05/2017.
 */
public final class Master {
	//================================================================================
	// Properties
	//================================================================================

	public static final Boolean DEBUG = true;
	public static final String TAG = "<Master>";
	public static final String HOSTNAME = "130.56.250.14";
	//public static final String HOSTNAME = "nfshome.duckdns.org";
	public static final int PORT = 8081;

	public static final int MINIMUM_JOB_DEADLINE_MS_FROM_NOW = 300000; // 5 minutes

	private static Map<String, Job> inactiveJobs;
	private static Map<String, Job> activeJobs;

	private static Map<WorkerType, LinkedList<Job>> queuedUnassignedJobs;
	private static Map<WorkerType, Map<String, Worker>> workers;

	// This Map of Booleans is used to keep track of which WorkerTypes are assigning jobs
	// - Only one job per WorkerType can be assigning at any one time
	private static volatile Map<WorkerType, Boolean> isAssigningJob;

	private static volatile Map<WorkerType, Boolean> isCreatingNewWorker;

	private static JCloudsNova jCloudsNova;
	private static Boolean initiated = false;

	static {
		inactiveJobs = Collections.synchronizedMap(new HashMap<String, Job>());
		activeJobs = Collections.synchronizedMap(new HashMap<String, Job>());

		isAssigningJob = Collections.synchronizedMap(new HashMap<WorkerType, Boolean>());
		isCreatingNewWorker = Collections.synchronizedMap(new HashMap<WorkerType, Boolean>());

		queuedUnassignedJobs = Collections.synchronizedMap(new HashMap<WorkerType, LinkedList<Job>>());
		workers = Collections.synchronizedMap(new HashMap<WorkerType, Map<String, Worker>>());

		for (WorkerType workerType : WorkerType.values()) {
			isAssigningJob.put(workerType, false);
			isCreatingNewWorker.put(workerType, false);
			queuedUnassignedJobs.put(workerType, new LinkedList<Job>());
			workers.put(workerType, Collections.synchronizedMap(new HashMap<String, Worker>()));
		}
	}

	public static void init(){
		if (!initiated) {
			jCloudsNova = new JCloudsNova();

			initiated=true;

			//Worker worker = new Worker("nfsspark-test-worker", "130.56.250.14", 8081, WorkerType.PRIVATE, "bd7eec8f-bccd-4d53-9371-ec8871df917c", JCloudsNova.getDefaultFlavour());
			//workers.get(WorkerType.PRIVATE).put(worker.getWorkerId(), worker);

			//new Thread(worker).start();

			Master.testVaryDeadlines();
		}
	}

	public static void shutdown() {
		System.out.println(TAG+" Shutting down all worker threads...");
		// For each worker map -> for each worker -> stopRunning()
		workers.forEach((workerType,workers)->workers.forEach((id, worker)->worker.stopRunning()));

		jCloudsNova.terminateAll();
	}

	static void testVaryDeadlines() {
		String jobTemplate = "small-test(94)";
		System.out.println(TAG+" Executing test with varying deadlines");

		int numJobs = 10;
		final int deadlineModifier = 4;

		new Thread(()->{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (int i = 0; i<numJobs; i++) {
				new Thread(() -> {
					Job newJob = createJob();

					newJob.loadTemplate(jobTemplate);

					Calendar deadline = Calendar.getInstance();

					// Deadline = estimated execution time for job on largest flavour MULTIPLIED BY the deadlineModifier
					deadline.add(Calendar.MILLISECOND,
							deadlineModifier*newJob.getEstimatedExecutionTimeForFlavourInMs(JCloudsNova.getLargestFlavour()).intValue());

					activateJob(newJob.getId(), (JsonObject) new JsonParser()
							.parse("{\"deadline\":\""+
									Job.deadlineDateTimeStringFormat.format(deadline.getTime())+"\"}"));
				}).start();

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	//================================================================================
	// Job REST "Accessors"
	//================================================================================

	public static boolean updateJobStatusFromWorkerNode(String jobId, String jobStatus) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.updateStatusFromWorkerNode(jobStatus);
		}
		return false;
	}

	public static Path getJobConfigFile(String jobId) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getConfigFile();
		}
		return null;
	}

	public static String getJobConfigJsonString(String jobId) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getConfigJsonString();
		}
		return "";
	}

	public static boolean updateJobConfig(String jobId, String jsonString) {
		Job job = getJob(jobId);
		if (job!=null) {
			job.updateConfigFromJsonString(jsonString);
			return true;
		}
		return false;
	}

	public static Path getJobDir(String jobId, String directoryName) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getDirectory(directoryName);
		}
		return null;
	}

	// Get Job Resources Directory - which contains all files required for job execution
	public static Path getJobResourcesDir(String jobId) {
		return getJobDir(jobId, "resources");
	}

	// Get Job Results Directory - which contains all files uploaded from worker after execution
	public static Path getJobResultsDir(String jobId) {
		return getJobDir(jobId, "results");
	}

	public static JsonObject processJobNewUploadedFiles(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			return job.processNewUploadedFilesInResourcesDir();
		}
		return null;
	}

	// Get list of jobs in JSON
	public static JsonArray getActiveJobsListJSON() {
		return getJobsListJSON(activeJobs);
	}
	public static JsonArray getInactiveJobsListJSON() {
		return getJobsListJSON(inactiveJobs);
	}
	private static JsonArray getJobsListJSON(Map<String, Job> jobMap) {
		JsonArray obj = new JsonArray();
		jobMap.forEach((jobId,job) -> obj.add(job.getSerializedJsonElement()));

		return obj;
	}

	//================================================================================
	// Worker REST "Accessors"
	//================================================================================

	public static boolean updateWorkerStatusFromWorkerNode(String workerId, String workerStatus) {
		Worker worker = getWorker(workerId);
		if (worker!=null) {
			return worker.updateStatusFromWorkerNode(workerStatus);
		}
		return false;
	}

	//================================================================================
	// Job List Functions - All methods are synchronised
	//================================================================================

	// Get job - using job id
	private static synchronized Job getJob(String jobId) {
		Job job = getActiveJob(jobId);
		if (job == null) {
			job = getInactiveJob(jobId);
		}
		return job;
	}

	private static synchronized Job getActiveJob(String jobId) {
		if (activeJobs.containsKey(jobId)) {
			return activeJobs.get(jobId);
		}
		return null;
	}
	private static synchronized Job getInactiveJob(String jobId) {
		if (inactiveJobs.containsKey(jobId)) {
			return inactiveJobs.get(jobId);
		}
		return null;
	}

	private static synchronized void addJobToActiveJobList(Job job) {
		activeJobs.put(job.getId(), job);
	}

	private static synchronized void addJobToInactiveJobList(Job job) {
		inactiveJobs.put(job.getId(), job);
	}

	private static synchronized void removeJobFromActiveJobList(Job job) {
		if (activeJobs.containsKey(job.getId())) {
			activeJobs.remove(job.getId());
		}
	}

	private static synchronized void removeJobFromInactiveJobList(Job job) {
		if (inactiveJobs.containsKey(job.getId())) {
			inactiveJobs.remove(job.getId());
		}
	}

	//================================================================================
	// Worker List Functions - All methods are synchronised
	//================================================================================

	// Get Worker - using worker id
	private static synchronized Worker getWorker(String workerId) {
		return workers.entrySet().stream()
				.filter(workerMap -> workerMap.getValue().containsKey(workerId))
				.findFirst().map(workerMap -> workerMap.getValue().get(workerId))
				.orElse(null);
	}

	private static synchronized Worker getWorker(String workerId, WorkerType workerType) {
		if (workers.get(workerType).containsKey(workerId)) {
			return workers.get(workerType).get(workerId);
		}
		return null;
	}

	private static synchronized void addWorker(Worker worker) {
		workers.get(worker.getType()).put(worker.getWorkerId(), worker);
	}

	private static synchronized void removePublicWorkerFromList(Worker worker) {
		workers.get(worker.getType()).remove(worker.getWorkerId());
	}

	//================================================================================
	// Job Functions
	//================================================================================
	public static Job createJob() {
		String newJobId = UUID.randomUUID().toString();
		Job newJob = new SparkJob(newJobId);
		addJobToInactiveJobList(newJob);
		return newJob;
	}

	public static Boolean activateJob(String jobId, JsonObject launchOptions) {
		Job job = getInactiveJob(jobId);
		if (job != null) {
			job.activateWithJson(launchOptions);

			removeJobFromInactiveJobList(job);
			addJobToActiveJobList(job);
			assignJobToWorker(job);
			return true;
		}
		return false;
	}

	private static void assignJob(Job job, Worker worker) {
		job.assignToWorker(worker);
		job.setOnStatusChangeListener((job1, currentStatus) -> {
			switch (currentStatus) {
				case REJECTED_BY_WORKER:
					assignJobToWorker(job1, true, false);
					break;
				case RUNNING:
					job1.setOnStatusChangeListener(null);
					break;
			}
		});
		worker.assignJob(job);
	}

	public static Boolean stopJob(String jobId) {
		Job job = getActiveJob(jobId);
		if (job != null) {
			removeJobFromActiveJobList(job);
			addJobToInactiveJobList(job);
			if (job.getStatus() != JobStatus.FINISHED && job.getUsedCpuTimeInMs() == 0) {
				job.stop();
				return true;
			}
		}
		return false;
	}

	public static Boolean deleteJob(String jobId) {
		Job job = getActiveJob(jobId);
		if (job != null) {
			removeJobFromActiveJobList(job);
		} else {
			job = getInactiveJob(jobId);
			if (job != null) {
				removeJobFromInactiveJobList(job);
			}
		}

		if (job != null) {
			job.delete();

			return true;
		}

		return false;
	}

	//================================================================================
	// Worker Functions
	//================================================================================

	private static Worker createNewWorker(WorkerType workerType, Flavor workerFlavour) {
		return createNewWorker(workerType, workerFlavour, 1);
	}
	private static Worker createNewWorker(WorkerType workerType, Flavor workerFlavour, int attempt) {
		isCreatingNewWorker.put(workerType, true);
		System.out.println(TAG+" create new "+workerType.toString()+" worker ");

		Worker newWorker = new Worker(workerFlavour, workerType);
		addWorker(newWorker);

		newWorker.setOnStatusChangeListener((worker, currentStatus) -> {
			switch (currentStatus) {
				// Worker created successfully & connected to
				case ACTIVE:
					System.out.println(TAG + " new "+workerType.toString()+" worker created!");
					worker.setOnStatusChangeListener(null);
					break;

				// Worker creation failed
				case CREATE_FAIL:
					System.out.println(TAG+" failed to create "+workerType.toString()+" rejecting all jobs");
					newWorker.rejectAllUncompletedJobs();
					break;

				// Worker created successfully - now waiting for instance...
				case CREATING:
					isCreatingNewWorker.put(workerType, false);
					continueAssigning(workerType);
					break;
			}
		});

		new Thread(newWorker).start();

		return newWorker;
	}

	public static synchronized void assignJobToWorker(final Job job) {
		assignJobToWorker(job, false, false);
	}

	private static synchronized void assignJobToWorker(final Job job, Boolean placeFirstInQueue, Boolean continueAssigning) {
		job.preparingForAssigning();
		WorkerType workerType = job.getWorkerType();
		if ((!isAssigningJob.get(workerType) && !isCreatingNewWorker.get(workerType)) || continueAssigning)
		{
			isAssigningJob.put(workerType, true);
			Worker mostFreeWorker = null;

			if (workers.get(workerType).size() > 0) {
				switch (workerType) {
					case PRIVATE:
						// Find worker with shortest queue - and worker which will allow job to finish before deadline
						mostFreeWorker = workers.get(workerType).values().stream()
								// Filter workers by:
								// Estimated queue completion time IS LESS THAN Earliest job start time
								.filter(worker -> worker.estimateQueueCompletionTimeInMs() <=
												job.getEarliestStartTimeForFlavorInMsFromNow(worker.getInstanceFlavour()))
								// Sort all available workers to find the worker with the smallest queue completion time
								.sorted(Comparator.comparing(Worker::estimateQueueCompletionTimeInMs))
								.findFirst().orElse(null);
						break;
					case PUBLIC:
						mostFreeWorker = workers.get(workerType).values().stream()
								.filter(Worker::isAvailable)
								.sorted(Comparator.comparing(Worker::getNumJobs))
								.findFirst().orElse(null);
				}
			}

			// If a suitable worker was found - assign job
			if (mostFreeWorker != null) {
				assignJob(job, mostFreeWorker);

				// If there are inactive jobs to assign - keep assigning
				if (queuedUnassignedJobs.get(workerType).size() != 0) {
					assignJobToWorker(popJobFromAssignQueue(workerType), false, true);
				} else {
					isAssigningJob.put(workerType, false);
				}
			}
			// If no suitable worker was found - create new worker and assign job
			else {
				Flavor workerFlavour = null;
				switch (workerType) {
					case PRIVATE:
						workerFlavour = JCloudsNova.getFlavours().values().stream()
								.sorted(Comparator.comparingLong(job::getEarliestStartTimeForFlavorInMsFromNow))
								.findFirst().orElse(null);
						break;
				}

				if (workerFlavour == null) {
					workerFlavour = JCloudsNova.getDefaultFlavour();
				}

				assignJob(job, createNewWorker(workerType, workerFlavour));
				isAssigningJob.put(workerType, false);
			}

		}
		// If already assigning a job - add to queue
		else {
			System.out.println(TAG+" can't assign "+workerType.toString()+" job - add to queue");
			addJobToAssignQueue(job, placeFirstInQueue);
		}
	}

	private synchronized static void continueAssigning(WorkerType workerType) {
		if (queuedUnassignedJobs.get(workerType).size() != 0)
			assignJobToWorker(popJobFromAssignQueue(workerType), false, true);
	}

	private synchronized static void addJobToAssignQueue(Job job) {
		addJobToAssignQueue(job, false);
	}
	private synchronized static void addJobToAssignQueue(Job job, Boolean placeFirstInQueue) {
		WorkerType workerType = job.getWorkerType();
		if (placeFirstInQueue) {
			queuedUnassignedJobs.get(workerType).addFirst(job);
		} else {
			queuedUnassignedJobs.get(workerType).addLast(job);
		}

		System.out.printf("%s job %s added to inactive job list \n queued "+workerType.toString()+" worker job count = %d%n", TAG, job.getId(), queuedUnassignedJobs.get(workerType).size());
	}

	private synchronized static Job popJobFromAssignQueue(WorkerType workerType) {
		Job job = queuedUnassignedJobs.get(workerType).removeFirst();
		System.out.printf("%s now assigning job %s \n queued "+workerType.toString()+" worker job count = %d%n", TAG, job.getId(), queuedUnassignedJobs.get(workerType).size());
		return job;
	}

	// When a worker is free (has 0 active jobs) -> Reject a job from the worker with the most jobs (if it has more than 2 jobs)
	public synchronized static void workerIsFree(WorkerType workerType) {
		System.out.printf("%s worker is free%n", TAG);
		try {
			workers.get(workerType).values().stream()
					.filter(worker -> worker.getNumJobs() > 2)
					.sorted(Comparator.comparingInt(Worker::getNumJobs).reversed()).findFirst().ifPresent(Worker::rejectMostRecentUncompletedJob);
		} catch (NullPointerException ignored) {

		}
	}
}
