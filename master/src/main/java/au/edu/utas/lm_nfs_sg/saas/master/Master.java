package au.edu.utas.lm_nfs_sg.saas.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by nico on 25/05/2017.
 */
public final class Master {
	public static final String  TAG = "<Master>";

	private static LinkedList<Job> inactiveJobs;
	private static HashMap<String, Job> activeJobs;
	private static LinkedList<MasterWorkerThread> activeWorkers;

	private static volatile Boolean creatingNewWorker = false;
	private static volatile Boolean assigningJob = false;
	private static volatile int workerResourcesReceivedCount;

	private static volatile LinkedList<Calendar> last100NewJobsCreatedDates;
	private static volatile LinkedList<Calendar> last100NewJobsFinishedDates;

	public static String test(){
		return "testsetset";
	}

	public static void main(String[] args)
	{
		last100NewJobsCreatedDates = new LinkedList<Calendar>();
		last100NewJobsFinishedDates = new LinkedList<Calendar>();

		inactiveJobs = new LinkedList<Job>();
		activeJobs = new HashMap<String, Job>();
		activeWorkers = new LinkedList<MasterWorkerThread>();

		while(true) {
			try {
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				// Read message from console
				String message = in.readLine();
				String command = message.split(" ")[0];

				String jobId;
				if (command.equals("new")) {
					System.out.println(TAG + " New job id = " +
							createJob(Integer.parseInt(message.split(" ")[1]), message.split(" ")[2], Integer.parseInt(message.split(" ")[3])));

				} else if (command.equals("results")) {
					getJobResults(message.split(" ")[1]);

				} else if (command.equals("workers_status")) {
					printWorkerStatus();

				} else if (command.equals("jobs_status")) {
					printJobStatus();

				} else if (command.equals("stop")) {
					finishJob(message.split(" ")[1]);

				} else if (command.equals("add_worker")) {
					addNewActiveWorker(new MasterWorkerThread(message.split(" ")[1], Integer.parseInt(message.split(" ")[2]), false));

				} else if (command.equals("add_cloud_worker")) {
					addNewActiveWorker(new MasterWorkerThread(message.split(" ")[1], Integer.parseInt(message.split(" ")[2]), true));

				}


			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static HashMap<String, Job> getActiveJobs() {
		return activeJobs;
	}

	static String createJob(int kwords, String streamHostname, int streamPort) {
		String newJobId = UUID.randomUUID().toString();
		Job newJob = new Job(newJobId, streamHostname, streamPort, kwords);
		newJob.setOnStatusChangeListener(new Job.StatusChangeListener() {
			@Override
			public void onStatusChanged(Job job, Job.Status currentStatus) {
				onJobStatusChanged(job, currentStatus);
			}
		});
		getAllWorkerCpuUsage(newJob);
		return newJobId;
	}

	static synchronized String jobSocketPortReceived(String id, int port) {
		Job job = activeJobs.get(id);
		if (job.getStatus() != Job.Status.ACTIVE) {
			job.setStartCpuTime();
			job.setWorkerProcessPort(port);
			job.setStatus(Job.Status.ACTIVE);
			return "created";
		} else {
			System.out.println(TAG+" updated job "+job.getId()+" port to "+port);
			job.setStatus(Job.Status.MIGRATING);
			job.setStatus(Job.Status.ACTIVE);
			job.setWorkerProcessPort(port);
			return "updated";
		}
	}

	static String getJobResults(String jobId) {
		return getJobResults(jobId, false);
	}
	static String getJobResults(String jobId, Boolean forceNewResults) {
		if (activeJobs.containsKey(jobId)) {
			Job job = activeJobs.get(jobId);
			// If job has no results OR forced result update -- AND -- current job results are complete (ie results aren't currently being retrieved)
			// -- AND job is currently ACTIVE
			if ((job.getResults() == null || forceNewResults) && !job.isCurrentlyRetrievingResults() && job.getStatus()== Job.Status.ACTIVE) {
				if (startWorkerThread(job.getWorker())) {
					job.getWorker().getResultsOnce(job);
					return "success";
				} else
					return "connection_timeout";
			}
			else {
				return job.getStatusString()+"\n\n"+job.getResultsString();
			}
		} else {
			System.out.println(TAG + " " + jobId + " is not a valid active job id");
			return "incorrect_id";
		}
	}

	static String finishJob(String jobId) {
		if (activeJobs.containsKey(jobId)) {
			Job job = activeJobs.get(jobId);
			if (job.getStatus() != Job.Status.FINISHED && job.getCpuTimeMs() == 0) {
				if (startWorkerThread(job.getWorker())) {
					job.getWorker().finishJob(job);
					if (last100NewJobsFinishedDates.size() == 100)
						last100NewJobsFinishedDates.removeFirst();
					last100NewJobsFinishedDates.addLast(Calendar.getInstance());
					return "success";
				} else
					return "connection_timeout";
			} else {
				return job.getStatusString()+"\n\n"+job.getCpuTimeMs()+" ms used \n"+job.getBill();
			}
		} else {
			return "incorrect_id";
		}
	}

	private static synchronized void addNewActiveWorker(MasterWorkerThread newWorker) {
		newWorker.setOnStatusChangeListener(Master::onWorkerStatusChanged);
		activeWorkers.add(newWorker);
	}

	private static void createWorker() {
		creatingNewWorker = true;
		MasterWorkerThread newWorker = new MasterWorkerThread(calculateNewWorkerVCpuCount());
		System.out.println(TAG+" begin creating new worker");
		new Thread(newWorker).start();
		activeWorkers.add(newWorker);
	}

	private static synchronized int calculateNewWorkerVCpuCount() {
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

	static void createWorkerFailed(MasterWorkerThread worker) {
		activeWorkers.remove(worker);
		System.out.println(TAG+" failed to create worker - retrying");
		createWorker();
	}

	static void newWorkerCreated() {
		// Syncrhonize on assigningJob?
		creatingNewWorker = false;
		assigningJob = true;
		System.out.println(TAG+" new worker created!");
		if (inactiveJobs.size() > 0) {
			System.out.println(TAG+" assigning inactive job");
			getAllWorkerCpuUsage(inactiveJobs.removeFirst(), true);
		}
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

	private static void getAllWorkerCpuUsage(final Job job) {
		getAllWorkerCpuUsage(job, false);}
	private static void getAllWorkerCpuUsage(final Job job, Boolean continueAssigning) {
		if ((!assigningJob && !creatingNewWorker && activeWorkers.size() > 0)||continueAssigning) {
			assigningJob = true;
			workerResourcesReceivedCount = 0;
			Iterator<MasterWorkerThread> activeWorkerIterator = activeWorkers.iterator();
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
			inactiveJobs.add(job);
			System.out.println(TAG+" already assigning a job - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+inactiveJobs.size());
		} else if (creatingNewWorker) {
			inactiveJobs.add(job);
			System.out.println(TAG+" no workers available - master already creating a new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+inactiveJobs.size());
		} else {
			inactiveJobs.add(job);
			System.out.println(TAG+" no worker available - create new worker - job "+job.getId()+ " added to inactive job list");
			System.out.println(TAG+" inactive job count = "+inactiveJobs.size());
			createWorker();
		}
	}

	private static void workerResourcesReceived(Job job) {
		workerResourcesReceivedCount++;
		if (workerResourcesReceivedCount >= activeWorkers.size())
			assignJobToMostFreeWorker(job);
	}

	private static synchronized void assignJobToMostFreeWorker(Job job) {
		// Find worker with least CPU usage
		Iterator<MasterWorkerThread> activeWorkerIterator = activeWorkers.iterator();
		MasterWorkerThread mostFreeWorker = null;
		while (activeWorkerIterator.hasNext()) {
			MasterWorkerThread currentWorker = activeWorkerIterator.next();
			if (mostFreeWorker == null  && currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE)
				mostFreeWorker = currentWorker;
			else if (currentWorker.getStatus() == MasterWorkerThread.Status.ACTIVE && currentWorker.getCpuUsage() < mostFreeWorker.getCpuUsage())
				mostFreeWorker = currentWorker;
		}

		if (mostFreeWorker != null && mostFreeWorker.getCpuUsage() < 0.8) {
			if (startWorkerThread(mostFreeWorker)) {
				System.out.println(TAG + " workerThread " + mostFreeWorker.getWorkerHost() + " is running");
				if (mostFreeWorker.createNewJob(job)) {
					activeJobs.put(job.getId(), job);
					job.setWorkerProcess(mostFreeWorker);
					job.setStatus(Job.Status.INITIATING);
					if (last100NewJobsCreatedDates.size() == 100)
						last100NewJobsCreatedDates.removeFirst();
					last100NewJobsCreatedDates.addLast(Calendar.getInstance());
				} else {
					inactiveJobs.add(job);
					System.out.println(TAG + " could not start job " + job.getId());
					System.out.println(TAG + " inactive job count = " + inactiveJobs.size());
				}
				// If there are inactive jobs to assign - keep assigning
				if (inactiveJobs.size() != 0) {
					System.out.println(TAG + " trying to assign an inactive job");
					getAllWorkerCpuUsage(inactiveJobs.removeFirst(), true);
				}
			}
		} else {
			inactiveJobs.add(job);
			System.out.println(TAG + " workers available");
			System.out.println(TAG + " could not start job " + job.getId());
			System.out.println(TAG + " inactive job count = " + inactiveJobs.size());
			createWorker();
		}
		assigningJob = false;
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
		for (Job job2 : inactiveJobs) {
			returnStr += job2.getId()+"\t"+job2.getStatus().toString()+"\n";
		}
		System.out.println(returnStr);
		return returnStr;
	}

	static synchronized String printWorkerStatus() {
		String returnStr = "";
		returnStr += "ACTIVE workers\n";
		for (MasterWorkerThread worker : activeWorkers) {
			returnStr += worker.getWorkerHost()+"\t"+worker.getStatus().toString()+"\n";
		}
		System.out.println(returnStr);
		return returnStr;
	}

}
