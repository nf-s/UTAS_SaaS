package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.gson.*;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;

import java.io.File;
import java.util.*;

/**
 * Created by nico on 25/05/2017.
 */
public final class Master {
	//================================================================================
	// Properties
	//================================================================================

	public static final String TAG = "<Master>";
	public static final String HOSTNAME = "130.56.250.15";
	public static final int PORT = 8081;

	private static LinkedList<Job> queuedSharedWorkerJobs;
	private static LinkedList<Job> queuedUnsharedWorkerJobs;

	private static Map<String, Job> inactiveJobs = Collections.synchronizedMap(new HashMap<String, Job>());
	private static Map<String, Job> activeJobs = Collections.synchronizedMap(new HashMap<String, Job>());

	private static LinkedList<MasterWorkerThread> activeSharedWorkers;
	private static LinkedList<MasterWorkerThread> activeUnsharedWorkers;

	private static volatile Boolean creatingNewSharedWorker = false;
	private static volatile Boolean creatingNewUnsharedWorker = false;
	private static volatile Boolean assigningJobToSharedWorker = false;
	private static volatile Boolean assigningJobToUnsharedWorker = false;


	private static JCloudsNova jCloudsNova;
	private static Boolean initiated = false;

	static {
		queuedSharedWorkerJobs = new LinkedList<Job>();
		queuedUnsharedWorkerJobs = new LinkedList<Job>();

		activeSharedWorkers = new LinkedList<MasterWorkerThread>();
		activeUnsharedWorkers = new LinkedList<MasterWorkerThread>();
	}

	public static void init(){
		if (!initiated) {
			jCloudsNova = new JCloudsNova();

			initiated=true;

			//activeUnsharedWorkers.add(new MasterWorkerThread("130.56.250.15", 8081, true, false));
			//activeUnsharedWorkers.getFirst().startThread();
		}
	}

	public static void main(String[] args)
	{
		/*
		while(true) {
			try {
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				// Read message from console
				String message = in.readLine();
				String command = message.split(" ")[0];

				String jobId;
				if (command.equals("new")) {
					System.out.println(TAG + " New job id = " +
							initJob(Integer.parseInt(message.split(" ")[1]), message.split(" ")[2], Integer.parseInt(message.split(" ")[3])));

				} else if (command.equals("workers_status")) {
					printWorkerStatus();

				} else if (command.equals("jobs_status")) {
					printJobStatus();

				} else if (command.equals("stop")) {
					stopJob(message.split(" ")[1]);

				} else if (command.equals("add_worker")) {
					addNewActiveWorker(new MasterWorkerThread(message.split(" ")[1], Integer.parseInt(message.split(" ")[2]), false));

				} else if (command.equals("add_cloud_worker")) {
					addNewActiveWorker(new MasterWorkerThread(message.split(" ")[1], Integer.parseInt(message.split(" ")[2]), true));

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		*/
	}

	//================================================================================
	// Job REST "Accessors"
	//================================================================================

	public static boolean updateJobStatus(String jobId, String jobStatus) {
		Job job = getJob(jobId);
		if (job!=null) {
			try {
				job.setStatus(Job.Status.valueOf(jobStatus));
				return true;
			} catch (IllegalArgumentException e) {
				System.out.println(TAG+" Illegal Argument Exception - Setting job status to "+jobStatus+" - jobId="+jobId);
			}
		}
		return false;
	}

	public static File getJobConfigFile(String jobId) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getJobConfigFile();
		}
		return null;
	}

	public static String getJobConfigJsonString(String jobId) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getJobConfigJsonString();
		}
		return "";
	}

	public static boolean updateJobConfig(String jobId, String jsonString) {
		Job job = getJob(jobId);
		if (job!=null) {
			job.setJobConfigJsonString(jsonString);
			return true;
		}
		return false;
	}

	// Get Job Resources Directory - which contains all files required for job execution
	public static File getJobResourcesDir(String jobId) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getJobResourcesDirectory();
		}
		return null;
	}

	// Get Job Results Directory - which contains all files uploaded from worker after execution
	public static File getJobResultsDir(String jobId) {
		Job job = getJob(jobId);
		if (job!=null) {
			return job.getJobResultsDirectory();
		}
		return null;
	}

	public static JsonObject processInactiveJobResourcesDir(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			return job.processJobResourcesDir();
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
		GsonBuilder gsonB = new GsonBuilder();
		gsonB.registerTypeAdapter(Job.class, new JobJSONSerializer());
		Gson gson = gsonB.create();

		JsonArray obj = new JsonArray();
		jobMap.forEach((jobId,job) -> obj.add(gson.toJsonTree(job, Job.class)));

		return obj;
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
	// Job Functions
	//================================================================================
	public static Job createJob() {
		String newJobId = UUID.randomUUID().toString();
		Job newJob = new SparkJob(newJobId);
		addJobToInactiveJobList(newJob);
		return newJob;
	}


	public static Boolean initJob(String jobId, JsonObject launchOptions) {
		Job job = getInactiveJob(jobId);
		if (job != null) {
			job.setLaunchOptions(launchOptions);
			job.setStatus(Job.Status.INITIATING);
			addJobToActiveJobList(job);
			removeJobFromInactiveJobList(job);
			if (job.getRunOnSharedWorker()) {
				assignJobToSharedWorker(job);
			} else {
				queueJobToUnsharedWorker(job);
			}
			return true;
		}
		return false;
	}

	private static void activateJob(Job job, MasterWorkerThread worker) {
		job.setWorkerProcess(worker);
		job.setOnStatusChangeListener((job1, currentStatus) -> {
			if (currentStatus == Job.Status.PREPARING) {
				// Set status listener to Master
				job.setOnStatusChangeListener((job2, currentStatus1) -> onJobStatusChanged(job2, currentStatus1));
			} else if (currentStatus == Job.Status.ERROR) {
				// Hanlde this...
				System.out.println(job1.getId()+" could not be launched");
			}
		});
		worker.assignJob(job);
	}

	public static Boolean stopJob(String jobId) {
		Job job = getActiveJob(jobId);
		if (job != null) {
			removeJobFromActiveJobList(job);
			addJobToInactiveJobList(job);
			job.setStatus(Job.Status.STOPPING);
			if (job.getStatus() != Job.Status.FINISHED && job.getUsedCpuTimeInMs() == 0) {
				if (job.getWorker().startThread()) {
					job.getWorker().stopJob(job);
					return true;
				}
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
			MasterWorkerThread worker = job.getWorker();
			if (worker != null) {
				worker.deleteJob(job);
			}
			// Need to also delete from worker
			job.deleteJob();

			job = null;
			return true;
		}

		return false;
	}

	//================================================================================
	// Shared Worker Functions
	//================================================================================
	private static void createSharedWorker() {
		createSharedWorker(JCloudsNova.getDefaultFlavour());
	}
	private static void createSharedWorker(Flavor workerFlavour) {
		creatingNewSharedWorker = true;

		final MasterWorkerThread newWorker = new MasterWorkerThread(workerFlavour, true);

		newWorker.setOnStatusChangeListener((wrkr, currentStatus) -> {
			// Worker created successfully
			if (currentStatus == MasterWorkerThread.Status.ACTIVE) {
				activeSharedWorkers.add(newWorker);
				//assigningJobToSharedWorker = true;
				creatingNewSharedWorker = false;
				System.out.println(TAG + " new worker created!");

				if (queuedSharedWorkerJobs.size() > 0) {
					System.out.println(TAG + " assigning inactive job");
					assignJobToSharedWorker(queuedSharedWorkerJobs.removeFirst(), true);
				}
			}
			// Worker creation failed
			else if (currentStatus == MasterWorkerThread.Status.FAILURE) {
				System.out.println(TAG+" failed to create worker - retrying");
				createSharedWorker();
			}
		});

		System.out.println(TAG+" begin creating new worker");
		new Thread(newWorker).start();
	}

	private static synchronized int calculateNewWorkerVCpuCount() {
		/*
		long avgDiffInStartToFinish100Jobs = 0;
		long minutesBetweenLastAnd100thJob = 0;


		if (last100NewJobsCreatedDates.size() > 2 && last100NewJobsFinishedDates.size() > 2) {
			minutesBetweenLastAnd100thJob = last100NewJobsCreatedDates.size()*(last100NewJobsCreatedDates.getLast().getTimeInMillis()-last100NewJobsCreatedDates.getFirst().getTimeInMillis()/(1000*60));
			// Average time between finishing jobs / Average time between starting jobs
			avgDiffInStartToFinish100Jobs = (last100NewJobsFinishedDates.getFirst().getTimeInMillis() - last100NewJobsFinishedDates.getFirst().getTimeInMillis()) / (last100NewJobsCreatedDates.size())
					/ (last100NewJobsCreatedDates.getFirst().getTimeInMillis() - last100NewJobsCreatedDates.getFirst().getTimeInMillis()) / (last100NewJobsCreatedDates.size());

		}

		// i.e. if jobs are being started quicker than they are being finished/stopped
		if (avgDiffInStartToFinish100Jobs > 1 && minutesBetweenLastAnd100thJob > 60*100) {
			return nextPowerOf2((int) Math.floor(avgDiffInStartToFinish100Jobs));
		} else {
			return 1;
		}
		*/
		return 1;
	}

	// From https://stackoverflow.com/questions/5242533/fast-way-to-find-exponent-of-nearest-superior-power-of-2
	private static int nextPowerOf2(final int a)
	{
		int b = 1;
		while (b < a)
		{
			b = b << 1;
		}

		return b;
	}

	/*
	private static void getAllWorkerCpuUsage(final Job job) {
		getAllWorkerCpuUsage(job, false);}
	private static void getAllWorkerCpuUsage(final Job job, Boolean continueAssigning) {
		if ((!assigningJobToSharedWorker && !creatingNewSharedWorker && activeSharedWorkers.size() > 0)||continueAssigning) {
			assigningJobToSharedWorker = true;
			workerResourcesReceivedCount = 0;
			Iterator<MasterWorkerThread> activeWorkerIterator = activeSharedWorkers.iterator();
			while (activeWorkerIterator.hasNext()) {
				MasterWorkerThread currentWorker = activeWorkerIterator.next();
				if (startWorkerThread(currentWorker)) {
					currentWorker.setOnResourcesReceivedListener(worker -> {
						workerResourcesReceived(job);
					});
					currentWorker.setOnStatusChangeListener((worker, currentStatus) -> {
						if (currentStatus == MasterWorkerThread.Status.UNREACHABLE || currentStatus == MasterWorkerThread.Status.ERROR) {
							workerResourcesReceived(job);
							System.out.println(TAG+" WorkerThread could not connect to "+currentWorker.getWorkerHost());
						}
					});
					currentWorker.getCpuUsageFromWorker();
				} else {
					workerResourcesReceived(job);
					System.out.println(TAG+" problem starting worker thread "+currentWorker.getWorkerHost());
				}
			}
		} else if (assigningJobToSharedWorker) {
			queuedSharedWorkerJobs.add(job);
			System.out.println(TAG+" already assigning a job - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedSharedWorkerJobs.size());
		} else if (creatingNewSharedWorker) {
			queuedSharedWorkerJobs.add(job);
			System.out.println(TAG+" no workers available - master already creating a new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedSharedWorkerJobs.size());
		} else {
			queuedSharedWorkerJobs.add(job);
			System.out.println(TAG+" no worker available - create new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedSharedWorkerJobs.size());
			createSharedWorker();
		}
	}*/

	private static void assignJobToSharedWorker(final Job job) {
		assignJobToSharedWorker(job, false);
	}

	private static synchronized void assignJobToSharedWorker(final Job job, Boolean continueAssigning) {
		if ((!assigningJobToSharedWorker && !creatingNewSharedWorker && activeSharedWorkers.size() > 0)||continueAssigning) {
			assigningJobToSharedWorker = true;
			job.setStatus(Job.Status.ASSIGNING);
			// Find worker with least CPU usage
			Iterator<MasterWorkerThread> activeWorkerIterator = activeSharedWorkers.iterator();
			MasterWorkerThread mostFreeWorker = null;
			while (activeWorkerIterator.hasNext()) {
				MasterWorkerThread currentWorker = activeWorkerIterator.next();
				if (currentWorker.startThread()) {
					if (mostFreeWorker == null && currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE)
						mostFreeWorker = currentWorker;
					else if (currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE && currentWorker.getNumJobs() < mostFreeWorker.getNumJobs())
						mostFreeWorker = currentWorker;
				}
			}

			if (mostFreeWorker != null && mostFreeWorker.isAvailable()) {
				if (mostFreeWorker.startThread()) {
					System.out.println(TAG + " shared workerThread " + mostFreeWorker.getWorkerHost() + " is running");

					activateJob(job, mostFreeWorker);

					// If there are inactive jobs to assign - keep assigning
					if (queuedSharedWorkerJobs.size() != 0) {
						System.out.println(TAG + " trying to assign an inactive shared job");
						assignJobToSharedWorker(queuedSharedWorkerJobs.removeFirst(), true);
					}
				}
			}
			// If no workers available - create new worker
			else {
				job.setStatus(Job.Status.ASSIGNING, "Creating new worker");
				queuedSharedWorkerJobs.add(job);
				System.out.println(TAG + " no shared workers available");
				System.out.println(TAG + " could not start shared job " + job.getId());
				System.out.println(TAG + " inactive shared job count = " + queuedSharedWorkerJobs.size());
				createSharedWorker();
			}
			assigningJobToSharedWorker = false;
		}
		// If already assigning a job - add to queue
		else if (assigningJobToSharedWorker) {
			job.setStatus(Job.Status.ASSIGNING, "In queue");
			queuedSharedWorkerJobs.add(job);
			System.out.println(TAG+" already assigning a shared job - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive shared job count = "+ queuedSharedWorkerJobs.size());
		}
		// If already creating a worker
		else if (creatingNewSharedWorker) {
			job.setStatus(Job.Status.ASSIGNING, "Creating new worker - in queue");
			queuedSharedWorkerJobs.add(job);
			System.out.println(TAG+" no shared workers available - master already creating a new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive shared job count = "+ queuedSharedWorkerJobs.size());
		}
		// If no active workers
		else {
			job.setStatus(Job.Status.ASSIGNING, "Creating new worker");
			queuedSharedWorkerJobs.add(job);
			System.out.println(TAG+" no shared worker available - create new shared worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive shared job count = "+ queuedSharedWorkerJobs.size());
			createSharedWorker();
		}
	}

	//================================================================================
	// Unshared (i.e. queue jobs) Worker Functions
	//================================================================================

	private static void createUnsharedWorkerForJob(Job job) {
		createUnsharedWorkerForJob(job, JCloudsNova.getDefaultFlavour());
	}
	private static void createUnsharedWorkerForJob(Job job, Flavor workerFlavour) {
		creatingNewUnsharedWorker = true;

		System.out.println(TAG+" create new unshared worker - job "+job.getId()+ " will be assigned when worker has been created");

		job.setStatus(Job.Status.ASSIGNING, "Creating new worker");

		job.setEstimatedFinishDateInMsFromNow(job.getEstimatedExecutionTimeForFlavourInMs(workerFlavour)
				+JCloudsNova.estimateCreationTimeInMs(workerFlavour));

		final MasterWorkerThread newWorker = new MasterWorkerThread(workerFlavour, false);

		newWorker.setOnStatusChangeListener((wrkr, currentStatus) -> {
			// Worker created successfully
			if (currentStatus == MasterWorkerThread.Status.ACTIVE) {
				System.out.println(TAG + " new unshared worker created!");

				activateJob(job, newWorker);

				activeUnsharedWorkers.add(newWorker);
				creatingNewUnsharedWorker = false;

				if (queuedUnsharedWorkerJobs.size() > 0) {
					System.out.println(TAG + " assigning inactive unshared job");
					queueJobToUnsharedWorker(queuedUnsharedWorkerJobs.removeFirst(), true);
				}
			}
			// Worker creation failed
			else if (currentStatus == MasterWorkerThread.Status.FAILURE) {
				System.out.println(TAG+" failed to create unshared worker - retrying in 5 seconds");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				createUnsharedWorkerForJob(job);
			}
		});

		System.out.println(TAG+" begin creating new unshared worker");
		new Thread(newWorker).start();
	}

	private static synchronized void queueJobToUnsharedWorker(final Job job) {
		queueJobToUnsharedWorker(job, false);
	}

	private static synchronized void queueJobToUnsharedWorker(final Job job, Boolean continueAssigning) {
		if ((!assigningJobToUnsharedWorker && activeUnsharedWorkers.size() > 0) ||continueAssigning)
		{
			assigningJobToUnsharedWorker = true;
			job.setStatus(Job.Status.ASSIGNING);

			// Get estimated time (in ms from now) to create new cloud instance
			Long estimatedTimeToCreateWorker = JCloudsNova.estimateCreationTimeInMs();

			// Get job deadline in ms from now
			Long jobDeadline = Job.getCalendarInMsFromNow(job.getDeadline());

			// If job deadline is null or before estimated worker creation time (with default flavour)
			// => set job deadline to estimated worker creation time
			Long estimatedJobCompletionWithNewWorker =  estimatedTimeToCreateWorker+job.getEstimatedExecutionTimeForFlavourInMs(JCloudsNova.getDefaultFlavour());
			// - this is because creating a new worker MAY BE SLOWER than assigning job to a worker with queued jobs
			if (jobDeadline <= estimatedJobCompletionWithNewWorker) {
				jobDeadline = estimatedJobCompletionWithNewWorker;
			}

			// Declarations for variables that are assigned/used in while loop - and also used in following if statement
			MasterWorkerThread mostFreeWorker = null;

			// Find worker with shortest queue - and worker which will allow job to finish before deadline
			Iterator<MasterWorkerThread> activeWorkerIterator = activeUnsharedWorkers.iterator();

			while (activeWorkerIterator.hasNext()) {
				MasterWorkerThread currentWorker = activeWorkerIterator.next();
				if (currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE) {
					// Get job estimated execution time according to the worker's instance flavour (i.e. VM config - VPUs, RAM...)
					Long jobEstimatedExecutionTime = job.getEstimatedExecutionTimeForFlavourInMs(currentWorker.getInstanceFlavour());

					// Get job deadline start time (latest possible start time in order to be completed before deadline)
					Long jobDeadlineStartTime = jobDeadline-jobEstimatedExecutionTime;

					// Get worker estimated queue competion Time
					Long currentWorkerQueueCompleteTime = currentWorker.estimateQueueCompletionTimeInMs();

					// If current worker's job queue will finish in time - i.e. before job's latest possible start time
					if (currentWorkerQueueCompleteTime <= jobDeadlineStartTime) {
						if (mostFreeWorker == null)
							mostFreeWorker = currentWorker;
						// Should assign job to MOST FREE worker - i.e. worker with shortest job queue
						else if (currentWorkerQueueCompleteTime < mostFreeWorker.estimateQueueCompletionTimeInMs())
							mostFreeWorker = currentWorker;
					}
				}
			}

			// If a suitable worker was found
			if (mostFreeWorker != null && mostFreeWorker.isAvailable()) {
				if (mostFreeWorker.startThread()) {
					job.setEstimatedFinishDateInMsFromNow(mostFreeWorker.estimateQueueCompletionTimeInMs()
							+job.getEstimatedExecutionTimeForFlavourInMs(mostFreeWorker.getInstanceFlavour()));

					activateJob(job, mostFreeWorker);

					// If there are inactive jobs to assign - keep assigning
					if (queuedUnsharedWorkerJobs.size() != 0) {
						queueJobToUnsharedWorker(popJobFromUnsharedJobAssignQueue(), true);
					}
				}
			}
			// If no suitable worker was found AND not creating new worker
			else if (!creatingNewUnsharedWorker) {
				createUnsharedWorkerForJob(job);
			}
			// If no suitable worker was found AND already creating new worker - QUEUE JOB
			else {
				addJobToUnsharedJobAssignQueue(job);
			}

			assigningJobToUnsharedWorker = false;
		}
		// If already assigning a job - add to queue
		else if (assigningJobToUnsharedWorker) {
			System.out.println(TAG+" can't assign job - already assigning an unshared worker job");
			addJobToUnsharedJobAssignQueue(job);
		}
		// If already creating a worker
		else if (creatingNewSharedWorker) {
			System.out.println(TAG+" can't assign job - already creating a worker");
			addJobToUnsharedJobAssignQueue(job);
		}
		// If no active workers
		else {
			createUnsharedWorkerForJob(job);
		}
	}

	private synchronized static void addJobToUnsharedJobAssignQueue(Job job) {
		job.setStatus(Job.Status.ASSIGNING, "In queue");
		queuedUnsharedWorkerJobs.add(job);
		System.out.printf("%s job %s added to inactive job list \n queued unshared worker job count = %d%n", TAG, job.getId(), queuedUnsharedWorkerJobs.size());
	}

	private synchronized static Job popJobFromUnsharedJobAssignQueue() {
		Job job = queuedUnsharedWorkerJobs.removeFirst();
		System.out.printf("%s now assigning job %s \n queued unshared worker job count = %d%n", TAG, job.getId(), queuedUnsharedWorkerJobs.size());
		return job;
	}

	//================================================================================
	// Job/Worker Status changed functions
	//================================================================================


	private static void onJobStatusChanged(Job job, Job.Status currentStatus) {
		switch (currentStatus) {
			case ERROR:
				System.out.println(TAG+" job "+job.getId()+" encountered an error");
				break;
			case STARTING:
				System.out.println(TAG+" job "+job.getId()+" is starting");
				break;
			case PREPARING:
				System.out.println(TAG+" job "+job.getId()+" is preparing on worker");
				break;
			case RUNNING:
				System.out.println(TAG+" job "+job.getId()+" is running");
				break;
			case FINISHED:
				System.out.println(TAG+" job "+job.getId()+" is finished");
				System.out.println(TAG+" job "+job.getId()+" cpu time in ms = "+job.getUsedCpuTimeInMs());
				break;
			default:
				System.out.printf(String.format("%s job %s status updated to %s %n", TAG, job.getId(), currentStatus.toString()));
		}
	}

	private static void onWorkerStatusChanged(MasterWorkerThread worker, MasterWorkerThread.Status currentStatus) {
		switch (currentStatus) {
			case ERROR:
				System.out.println(TAG+" worker "+worker.getWorkerHost()+" encountered an error");
				break;
			case INITIATING:
				System.out.println(TAG+" worker "+worker.getWorkerHost()+" is initiating");
				break;
			case UNREACHABLE:
				System.out.println(TAG+" worker "+worker.getWorkerHost()+" is unreachable");
				break;
			case ACTIVE:
				System.out.println(TAG+" worker "+worker.getWorkerHost()+" is ACTIVE!!");
				break;
		}
	}

	//================================================================================
	// Debug functions
	//================================================================================

	static synchronized String printJobStatus() {
		String returnStr = "";
		returnStr += "ACTIVE jobs\n";
		for (Job job : activeJobs.values()) {
			returnStr += job.getId()+"\t"+job.getStatus().toString()+"\t"+job.getWorker().getWorkerHost()+"\n";
		}
		returnStr += "\nINACTIVE jobs\n";
		for (Job job2 : queuedSharedWorkerJobs) {
			returnStr += job2.getId()+"\t"+job2.getStatus().toString()+"\n";
		}
		System.out.println(returnStr);
		return returnStr;
	}

	static synchronized String printWorkerStatus() {
		String returnStr = "";
		returnStr += "ACTIVE workers\n";
		for (MasterWorkerThread worker : activeSharedWorkers) {
			returnStr += worker.getWorkerHost()+"\t"+worker.getStatus().toString()+"\n";
		}
		System.out.println(returnStr);
		return returnStr;
	}

}
