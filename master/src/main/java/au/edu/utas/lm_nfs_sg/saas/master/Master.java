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
import java.util.stream.Stream;

/**
 * Created by nico on 25/05/2017.
 */
public final class Master {
	//================================================================================
	// Properties
	//================================================================================

	public static final Boolean DEBUG = true;
	public static final Boolean TESTING = true;

	// Tag for debugging purposes
	public static final String TAG = "<Master>";

	// The hostname and port of the Master node (this MUST be correct as it is passed to all worker nodes on creation)
	public static final String HOSTNAME = "144.6.225.200";
	public static final int PORT = 8081;

	// The threshold for creating a new worker (i.e. a deadline must be over 3 minutes late inorder to trigger worker creation)
	public static final int MINIMUM_JOB_DEADLINE_MS_FROM_NOW = 180000; // 3 minutes

	private static Map<String, Job> inactiveJobs;
	private static Map<String, Job> activeJobs;

	// Map of queued jobs to assign for each WorkerType
	private static Map<WorkerType, LinkedList<Job>> queuedUnassignedJobs;
	// Map of workers for each WorkerType
	private static Map<WorkerType, Map<String, Worker>> workers;

	// This Map of Booleans is used to keep track of which WorkerTypes are assigning jobs
	// - Only one job per WorkerType can be assigning at any one time
	private static volatile Map<WorkerType, Boolean> isAssigningJob;

	// ... the same applies to creating new workers
	private static volatile Map<WorkerType, Boolean> isCreatingNewWorker;

	private static JCloudsNova jCloudsNova;

	private static Boolean initiated = false;

	// Synchronise objects
	private static final Boolean assignSynchronise = true;
	private static final Boolean jobAssignQueueSynchronise = true;


	static {
		// Initiate Worker, Job and Boolean maps/lists
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
			// A JCloudsNova object must be created to request information from NectarCloud APIs
			jCloudsNova = new JCloudsNova();

			initiated=true;

			/* Example of adding an existing worker node
					Worker worker = new Worker("nfsspark-test-worker", "130.56.250.14", 8081, WorkerType.PRIVATE, "bd7eec8f-bccd-4d53-9371-ec8871df917c", JCloudsNova.getDefaultFlavour());
					workers.get(WorkerType.PRIVATE).put(worker.getWorkerId(), worker);

					new Thread(worker).start();
			*/


			/* Example performance evaluation
					PerformanceEvaluation performanceEvaluation = new PerformanceEvaluation(jCloudsNova, 5, 4, 10, new String[]{"s", "m"});
					performanceEvaluation.addToQueue(new PerformanceEvaluation(jCloudsNova, 5, 4, 20, new String[]{"s", "m"}));
					performanceEvaluation.addToQueue(new PerformanceEvaluation(jCloudsNova, 5, 4, 30, new String[]{"s", "m"}));
					performanceEvaluation.addToQueue(new PerformanceEvaluation(jCloudsNova, 5, 4, 40, new String[]{"s", "m"}));

					performanceEvaluation.addToQueue(new PerformanceEvaluation(jCloudsNova, 5, 2, 10, new String[]{"s", "m"}));
					performanceEvaluation.addToQueue(new PerformanceEvaluation(jCloudsNova, 5, 8, 10, new String[]{"s", "m"}));
					performanceEvaluation.addToQueue(new PerformanceEvaluation(jCloudsNova, 5, 16, 10, new String[]{"s", "m"}));

					// Start first
					new Thread(performanceEvaluation).start();
					*/
		}
	}

	public static void shutdown() {
		System.out.println(TAG+" Shutting down all worker threads...");
		// For each worker map -> for each worker -> stopRunning()
		workers.forEach((workerType,workers)->workers.forEach((id, worker)->worker.stopRunning()));

		jCloudsNova.terminateAll();
	}

	//================================================================================
	// Job REST "Accessors"
	//
	// All of the following functions are called from REST Api
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

	private static synchronized void clearWorkersMaps() {
		workers.forEach((workerType, workerList) -> workerList.clear());
	}

	//================================================================================
	// Job Functions
	//================================================================================

	/**
	 * Returns a new SparkJob object with a new random UUID string and adds it to the inactive job list
	 *
	 * @return     Job
	 */
	public static Job createJob() {
		String newJobId = UUID.randomUUID().toString();
		Job newJob = new SparkJob(newJobId);
		addJobToInactiveJobList(newJob);
		return newJob;
	}

	/**
	 * Activates job with the given jobId, and launch options. The job is removed from the inactive job list and added
	 * to the active job list. The job is then assigned.
	 *
	 * @return      Boolean - True if job exists, false otherwise
	 */
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

	/**
	 * Assigns job to given worker. If a job is rejected by the worker, it is reassigned to another worker
	 */
	private static void assignedJob(Job job, Worker worker) {
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

	/**
	 * Stops job with given JobId, given job is already running. It is removed from the active job list and added to the
	 * inactive job list
	 *
	 * @return      Boolean - True if job exists, false otherwise
	 */
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

	/**
	 * Deletes job with given JobId
	 *
	 * @return      Boolean - True if job exists, false otherwise
	 */
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

	/**
	 * Create new worker with given workerType and workerFlavour.
	 *
	 * @return      Worker
	 */
	private static Worker createNewWorker(WorkerType workerType, Flavor workerFlavour) {
		return createNewWorker(workerType, workerFlavour, 1);
	}
	private static Worker createNewWorker(WorkerType workerType, Flavor workerFlavour, int attempt) {
		synchronized (assignSynchronise) {
			isCreatingNewWorker.put(workerType, true);
		}
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

				// Worker created successfully - now waiting for instance to start...
				case CREATING:
					synchronized (assignSynchronise) {
						isCreatingNewWorker.put(workerType, false);
					}
					continueAssigning(workerType);
					break;
			}
		});

		new Thread(newWorker).start();

		return newWorker;
	}

	/**
	 * Find suitable worker for job. If no worker exists, a new one is created.
	 *
	 */
	public static void assignJobToWorker(final Job job) {
		assignJobToWorker(job, false, false);
	}

	private static void assignJobToWorker(final Job job, Boolean placeFirstInQueue, Boolean continueAssigning) {
		job.preparingForAssigning();
		WorkerType workerType = job.getWorkerType();

		Boolean canAssignJob;
		synchronized (assignSynchronise) {
			canAssignJob = (!isAssigningJob.get(workerType) && !isCreatingNewWorker.get(workerType)) || continueAssigning;
			if (canAssignJob)
				isAssigningJob.put(workerType, true);
		}

		if (canAssignJob)
		{
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
				assignedJob(job, mostFreeWorker);

				// If there are inactive jobs to assign - keep assigning
				if (queuedUnassignedJobs.get(workerType).size() != 0) {
					assignJobToWorker(popJobFromAssignQueue(workerType), false, true);
				} else {
					synchronized (assignSynchronise) {
						isAssigningJob.put(workerType, false);
					}
				}
			}
			// If no suitable worker was found - create new worker and assign job
			else {
				Flavor workerFlavour = null;
				switch (workerType) {
					case PRIVATE:
						try {
							workerFlavour = JCloudsNova.getFlavours().values().stream()
									// If earliest start time is negative -> can't make deadline
									.filter(flavour -> job.getEarliestStartTimeForFlavorInMsFromNow(flavour) >= 0)
									.sorted(Comparator.comparingLong(job::getEarliestStartTimeForFlavorInMsFromNow))
									.findFirst().orElse(null);
						} catch (NullPointerException ignored) {

						}
						break;
				}

				if (workerFlavour == null) {
					workerFlavour = JCloudsNova.getLargestFlavour();
				}

				assignedJob(job, createNewWorker(workerType, workerFlavour));
				synchronized (assignSynchronise) {
					isAssigningJob.put(workerType, false);
				}
			}

		}
		// If already assigning a job - add to queue
		else {
			System.out.println(TAG+" can't assign "+workerType.toString()+" job - add to queue");
			addJobToAssignQueue(job, placeFirstInQueue);
		}
	}

	private static void continueAssigning(WorkerType workerType) {
		Job jobToAssign = null;
		synchronized (jobAssignQueueSynchronise) {
			if (queuedUnassignedJobs.get(workerType).size() != 0)
				jobToAssign = popJobFromAssignQueue(workerType);
		}

		if (jobToAssign != null)
			assignJobToWorker(jobToAssign, false, true);
	}

	private static void addJobToAssignQueue(Job job) {
		addJobToAssignQueue(job, false);
	}
	private static void addJobToAssignQueue(Job job, Boolean placeFirstInQueue) {
		synchronized (jobAssignQueueSynchronise) {
			WorkerType workerType = job.getWorkerType();
			if (placeFirstInQueue) {
				queuedUnassignedJobs.get(workerType).addFirst(job);
			} else {
				queuedUnassignedJobs.get(workerType).addLast(job);
			}

			System.out.printf("%s job %s added to inactive job list \n queued " + workerType.toString() + " worker job count = %d%n", TAG, job.getId(), queuedUnassignedJobs.get(workerType).size());
		}
	}

	private static Job popJobFromAssignQueue(WorkerType workerType) {
		synchronized (jobAssignQueueSynchronise) {
			Job job = queuedUnassignedJobs.get(workerType).removeFirst();
			System.out.printf("%s now assigning job %s \n queued " + workerType.toString() + " worker job count = %d%n", TAG, job.getId(), queuedUnassignedJobs.get(workerType).size());
			return job;
		}
	}

	public static void workerIsFree(WorkerType workerType) {
		synchronized (jobAssignQueueSynchronise) {
			System.out.printf("%s worker is free%n", TAG);

			try {
				workers.get(workerType).values().stream()
						.filter(worker -> worker.getNumJobs() > 2)
						.sorted(Comparator.comparingInt(Worker::getNumJobs).reversed())
						.findFirst().ifPresent(Worker::rejectMostRecentUncompletedJob);
			} catch (NullPointerException ignored) {

			}

			if (workers.get(workerType).values().stream()
					.filter(worker -> worker.getNumJobs() > 0).count() == 0) {
				allWorkersAreFree(workerType);
			}
		}
	}

	// When a worker is free (has 0 active jobs) -> Reject a job from the worker with the most jobs (if it has more than 2 jobs)
	public static void workerIsFreeNew(Worker freeWorker) {
		synchronized (jobAssignQueueSynchronise) {
			WorkerType freeWorkerType = freeWorker.getType();
			System.out.printf("%s worker is free%n", TAG);
			Job jobToAssign = null;
			try {
				jobToAssign = workers.get(freeWorkerType).values().stream()
						.filter(worker -> worker.getNumJobs() > 1)
						.flatMap(worker -> Stream.of(worker.getJobWithTightestDeadline()))
						.sorted(Comparator.comparingLong(o -> o.getImprovementInFinishTime(freeWorker.getInstanceFlavour())))
						.filter(o->o.getImprovementInFinishTime(freeWorker.getInstanceFlavour())<0).findFirst().orElse(null);

			} catch (NullPointerException ignored) {

			}

			if (jobToAssign != null) {
				jobToAssign.getWorker().rejectJob(jobToAssign);
				jobToAssign.assignToWorker(freeWorker);
			}

			if (workers.get(freeWorkerType).values().stream()
					.filter(worker -> worker.getNumJobs() > 0).count() == 0) {
				allWorkersAreFree(freeWorkerType);
			}
		}
	}

	public static void allWorkersAreFree(WorkerType workerType) {
		System.out.printf("%s all workers are free%n", TAG);

		if (TESTING) {
			ArrayList<String> results = new ArrayList<>();
			workers.get(workerType).forEach((workerId, worker)->
					results.add(String.format("%s Worker:%s CPU time used:%d Create time: %d%n", TAG, worker.getInstanceFlavour().getName(), worker.getUsedCpuTimeInMs(), worker.getCreateTimeInMs())));
			PerformanceEvaluation.getCurrentPerformanceEvaluation().addWorkerResults(results);
			clearWorkersMaps();
			PerformanceEvaluation.getCurrentPerformanceEvaluation().notifyThread();
		}
	}
}
