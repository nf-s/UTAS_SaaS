package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.*;

/**
 * Created by nico on 25/05/2017.
 */
public final class Master {
	public static final String  TAG = "<Master>";

	private static LinkedList<Job> queuedJobs;

	private static Map<String, Job> inactiveJobs = Collections.synchronizedMap(new HashMap<String, Job>());
	private static Map<String, Job> activeJobs = Collections.synchronizedMap(new HashMap<String, Job>());

	private static LinkedList<MasterWorkerThread> activeSharedWorkers;
	private static LinkedList<MasterWorkerThread> activeJobWorkers;

	private static volatile Boolean creatingNewWorker = false;
	private static volatile Boolean assigningJob = false;
	private static Boolean initiated = false;

	public static String test(){
		return "testsetset";
	}

	 static  {
		queuedJobs = new LinkedList<Job>();

		 activeSharedWorkers = new LinkedList<MasterWorkerThread>();
		 activeJobWorkers = new LinkedList<MasterWorkerThread>();
	}

	public static void init(){
		if (!initiated) {
			initiated=true;
			activeJobWorkers.add(new MasterWorkerThread("localhost", 1234, true));

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

	public static String getInactiveJobParamsJsonString(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			return job.getJobParamsJsonString();
		}
		return "";
	}

	public static boolean setInactiveJobParamsJsonString(String jobId, String jsonString) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			job.setJobParamsJsonString(jsonString);
			return true;
		}
		return false;
	}

	public static JSONObject processInactiveJobResourcesDir(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			return job.processJobResourcesDir();
		}
		return null;
	}

	public static JSONArray getInactiveJobResourcesDirFilenames(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			return job.getJobResourcesDirFilenames();
		}
		return null;
	}

	public static File getInactiveJobResourcesDir(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job!=null) {
			return job.getJobResourcesDirectory();
		}
		return null;
	}

	public static JSONArray getActiveJobsJSON() {
		return getJobsJSON(activeJobs);
	}

	public static JSONArray getInactiveJobsJSON() {
		return getJobsJSON(inactiveJobs);
	}

	private static JSONArray getJobsJSON(Map<String, Job> jobMap) {
		GsonBuilder gsonB = new GsonBuilder();
		gsonB.registerTypeAdapter(Job.class, new JobJSONSerializer());
		Gson gson = gsonB.create();

		JSONArray obj = new JSONArray();
		jobMap.forEach((jobId,job) -> obj.add(gson.toJsonTree(job, Job.class)));

		return obj;
	}

	protected static Job getInactiveJob(String jobId) {
		if (inactiveJobs.containsKey(jobId)) {
			return inactiveJobs.get(jobId);
		}
		return null;
	}

	public static Job createJob() {
		String newJobId = UUID.randomUUID().toString();
		Job newJob = new SparkJob(newJobId);
		inactiveJobs.put(newJobId, newJob);
		return newJob;
	}

	public static Boolean initJob(String jobId) {
		Job job = getInactiveJob(jobId);
		if (job != null) {
			inactiveJobs.remove(jobId);
			if (job.getJobRequiresOwnWorker()) {


				startWorkerThread(activeJobWorkers.getFirst());
				activateJob(job, activeJobWorkers.getFirst());

				//createWorkerForJob(job);
			} else {
				assignJobToMostFreeWorker(job);
			}
			return true;
		}
		return false;
	}

	public static Boolean stopJob(String jobId) {
		if (activeJobs.containsKey(jobId)) {
			Job job = activeJobs.get(jobId);
			activeJobs.remove(jobId);
			inactiveJobs.put(jobId, job);
			if (job.getStatus() != Job.Status.FINISHED && job.getCpuTimeMs() == 0) {
				if (startWorkerThread(job.getWorker())) {
					job.getWorker().finishJob(job);
					return true;
				}
			}
		}
		return false;
	}

	private static void createSharedWorker() {
		creatingNewWorker = true;
		//MasterWorkerThread newWorker = new MasterWorkerThread(calculateNewWorkerVCpuCount());
		MasterWorkerThread newWorker = new MasterWorkerThread(2);

		newWorker.setOnStatusChangeListener((worker, currentStatus) -> {
			// Worker created successfully
			if (currentStatus == MasterWorkerThread.Status.ACTIVE) {
				activeSharedWorkers.add(newWorker);
				//assigningJob = true;
				creatingNewWorker = false;
				System.out.println(TAG + " new worker created!");
				if (queuedJobs.size() > 0) {
					System.out.println(TAG + " assigning inactive job");
					assignJobToMostFreeWorker(queuedJobs.removeFirst(), true);
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


	private static void createWorkerForJob(Job job) {
		//MasterWorkerThread newWorker = new MasterWorkerThread(calculateNewWorkerVCpuCount());
		MasterWorkerThread newWorker = new MasterWorkerThread(2);

		newWorker.setOnStatusChangeListener((worker, currentStatus) -> {
			// Worker created successfully
			if (currentStatus == MasterWorkerThread.Status.ACTIVE) {
				activeJobWorkers.add(newWorker);
				System.out.println(TAG + " new job worker created!");

				activateJob(job, worker);
			}
			// Worker creation failed
			else if (currentStatus == MasterWorkerThread.Status.FAILURE) {
				System.out.println(TAG+" failed to create worker - retrying");
				createWorkerForJob(job);
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

	private static Boolean startWorkerThread(MasterWorkerThread worker) {
		if (worker.getStatus() != MasterWorkerThread.Status.CREATING) {
			if (!worker.isRunning())
				new Thread(worker).start();
			else if (!worker.isConnected())
				worker.notifyWorkerThread();

			while (worker.isConnecting()) {
				try {
					if (worker.isLastConnectionTimeoutRecent())
						break;
					Thread.sleep(500);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		return worker.isConnected();
	}

	/*
	private static void getAllWorkerCpuUsage(final Job job) {
		getAllWorkerCpuUsage(job, false);}
	private static void getAllWorkerCpuUsage(final Job job, Boolean continueAssigning) {
		if ((!assigningJob && !creatingNewWorker && activeSharedWorkers.size() > 0)||continueAssigning) {
			assigningJob = true;
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
		} else if (assigningJob) {
			queuedJobs.add(job);
			System.out.println(TAG+" already assigning a job - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedJobs.size());
		} else if (creatingNewWorker) {
			queuedJobs.add(job);
			System.out.println(TAG+" no workers available - master already creating a new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedJobs.size());
		} else {
			queuedJobs.add(job);
			System.out.println(TAG+" no worker available - create new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedJobs.size());
			createSharedWorker();
		}
	}*/

	private static void assignJobToMostFreeWorker(final Job job) {
		assignJobToMostFreeWorker(job, false);
	}

	private static synchronized void assignJobToMostFreeWorker(final Job job, Boolean continueAssigning) {
		if ((!assigningJob && !creatingNewWorker && activeSharedWorkers.size() > 0)||continueAssigning) {
			assigningJob = true;
			// Find worker with least CPU usage
			Iterator<MasterWorkerThread> activeWorkerIterator = activeSharedWorkers.iterator();
			MasterWorkerThread mostFreeWorker = null;
			while (activeWorkerIterator.hasNext()) {
				MasterWorkerThread currentWorker = activeWorkerIterator.next();
				if (mostFreeWorker == null && currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE)
					mostFreeWorker = currentWorker;
				else if (currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE && currentWorker.getNumJobs() < mostFreeWorker.getNumJobs())
					mostFreeWorker = currentWorker;
			}

			if (mostFreeWorker != null && mostFreeWorker.isAvailable()) {
				if (startWorkerThread(mostFreeWorker)) {
					System.out.println(TAG + " workerThread " + mostFreeWorker.getWorkerHost() + " is running");

					activateJob(job, mostFreeWorker);

					// If there are inactive jobs to assign - keep assigning
					if (queuedJobs.size() != 0) {
						System.out.println(TAG + " trying to assign an inactive job");
						assignJobToMostFreeWorker(queuedJobs.removeFirst(), true);
					}
				}
			}
			// If no workers available - create new worker
			else {
				queuedJobs.add(job);
				System.out.println(TAG + " workers available");
				System.out.println(TAG + " could not start job " + job.getId());
				System.out.println(TAG + " inactive job count = " + queuedJobs.size());
				createSharedWorker();
			}
			assigningJob = false;
		}
		// If already assigning a job - add to queue
		else if (assigningJob) {
			queuedJobs.add(job);
			System.out.println(TAG+" already assigning a job - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedJobs.size());
		}
		// If already creating a worker
		else if (creatingNewWorker) {
			queuedJobs.add(job);
			System.out.println(TAG+" no workers available - master already creating a new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedJobs.size());
		}
		// If no active workers
		else {
			queuedJobs.add(job);
			System.out.println(TAG+" no worker available - create new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+ queuedJobs.size());
			createSharedWorker();
		}
	}

	private static void activateJob(Job job, MasterWorkerThread worker) {
		activeJobs.put(job.getId(), job);
		job.setWorkerProcess(worker);
		job.setStatus(Job.Status.INITIATING);
		job.setOnStatusChangeListener((job1, currentStatus) -> {
			if (currentStatus == Job.Status.ACTIVE) {
				// Set status listener to Master
				job.setOnStatusChangeListener((job2, currentStatus1) -> onJobStatusChanged(job2, currentStatus1));
			} else if (currentStatus == Job.Status.ERROR) {
				// Hanlde this...
				System.out.println(job1.id+" could not be launched");
			}
		});
		worker.createNewJob(job);
	}


	private static void onJobStatusChanged(Job job, Job.Status currentStatus) {
		switch (currentStatus) {
			case ERROR:
				System.out.println(TAG+" job "+job.getId()+" encountered an error");
				break;
			case INITIATING:
				System.out.println(TAG+" job "+job.getId()+" is initiating");
				break;
			case UNREACHABLE:
				System.out.println(TAG+" job "+job.getId()+" is unreachable");
				break;
			case ACTIVE:
				System.out.println(TAG+" job "+job.getId()+" is active");
				break;
			case FINISHED:
				System.out.println(TAG+" job "+job.getId()+" is finished");
				System.out.println(TAG+" job "+job.getId()+" cpu time in ms = "+job.getCpuTimeMs());
				break;
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

	static synchronized String printJobStatus() {
		String returnStr = "";
		returnStr += "ACTIVE jobs\n";
		for (Job job : activeJobs.values()) {
			returnStr += job.getId()+"\t"+job.getStatus().toString()+"\t"+job.getWorker().getWorkerHost()+"\n";
		}
		returnStr += "\nINACTIVE jobs\n";
		for (Job job2 : queuedJobs) {
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
