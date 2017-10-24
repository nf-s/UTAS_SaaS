package au.edu.utas.lm_nfs_sg.saas.worker;

import au.edu.lm_nf_sg.saas.common.job.JobStatus;
import au.edu.lm_nf_sg.saas.common.worker.WorkerStatus;
import au.edu.lm_nf_sg.saas.common.worker.WorkerType;
import au.edu.utas.lm_nfs_sg.saas.comms.SocketServer1To1;
import au.edu.utas.lm_nfs_sg.saas.worker.rest.MasterRestClient;
import au.edu.utas.lm_nfs_sg.saas.worker.job.Job;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Collections.*;

public final class Worker {

	private static final String TAG = "<Worker>";

	private static String id;
	private static WorkerStatus status;
	private static WorkerType type;

	private static SocketServer1To1 masterSocketServer;
	private static MasterRestClient masterRestClient;

	private static String masterHostname;

	private static Map<String, Job> jobs;

	// Job queue (only for PRIVATE workers)
	// The first job is the currently running job
	private static Queue<Job> jobQueue;

	/**
	 *  ARGUMENTS (expects 4):
	 *  1 - Worker id (string)
	 *  2 - Master hostname (string)
	 *  3 - Master socket port (integer between 1024 and 65535)
	 *  4 - Worker Type (WorkerType enum - as string)
	 *
	 */
	public static void main(String[] args) {
		if (args.length >= 4) {
			status = WorkerStatus.INITIATING;

			id = args[0];
			masterHostname = args[1];

			int port = 0;
			try {
				port = Integer.parseInt(args[2]);
				// If invalid port number is provided - exit program
				if (port < 1024 || port > 65535) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				System.out.println("Argument '" + args[2] + "' is an invalid port number - expecting an integer between 1024 and 65535");
				System.exit(-1);
			}

			type = WorkerType.valueOf(args[3]);

			MasterRestClient.setMasterHostname(masterHostname);
			masterRestClient = new MasterRestClient("WorkerMain");

			masterSocketServer = new SocketServer1To1(TAG+" MasterSocketServer",port);
			masterSocketServer.setOnMessageReceivedListener(message -> handleSocketMessageFromMaster(message));

			Thread masterSocketThread = new Thread(masterSocketServer);
			masterSocketThread.start();

			jobs = synchronizedMap(new HashMap<String, Job>());
			if(type==WorkerType.PRIVATE) {
				jobQueue = new ConcurrentLinkedQueue<>();
			}

			setStatus(WorkerStatus.ACTIVE);

			try {
				masterSocketThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		// If less than 3 arguments are given when executing the program
		else {
			System.out.println("Incorrect number of arguments given");
			System.exit(-1);
		}
	}

	public static void sendMessageToMasterSocket(String mess) {
		masterSocketServer.sendMessage(mess);
	}

	private static void handleSocketMessageFromMaster(String mess) {
		System.out.println(masterSocketServer.getTag()+" received " +mess);

		String inputSplit;
		try
		{
			inputSplit = mess.split(" ")[0];
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			inputSplit = "";
		}

		switch (inputSplit) {
			case "new_job":
				launchNewJob(mess.split(" ")[1]);
				break;

			case "stop_job":
				stopJob(mess.split(" ")[1], false);
				break;

			case "delete_job":
				stopJob(mess.split(" ")[1], true);
				break;
		}
	}

	private static void setStatus(WorkerStatus newStatus) {
		if (status!=newStatus) {
			status = newStatus;
			masterRestClient.updateWorkerStatus(id, newStatus);
		}
	}

	private static Job getJob(String jobId) {
		if (jobs.containsKey(jobId)) {
			return jobs.get(jobId);
		} else {
			System.out.println(TAG+" couldn't find "+jobId);
		}
		return null;
	}

	private static void launchNewJob(String jobId) {
		System.out.println(TAG+" starting job "+jobId);

		Job newJob = new Job(jobId,true);

		jobs.put(jobId, newJob);

		switch(type) {
			// If this is a PRIVATE worker - only 1 job can be running at a time
			case PRIVATE:
				newJob.setOnStatusChangeListener((job, currentStatus) -> {
					if (currentStatus == JobStatus.FINISHING || currentStatus == JobStatus.ERROR) {
						popJobFromQueue(job);
						newJob.setOnStatusChangeListener(null);
					}
				});
				// If this job queue is empty - execute job straight away
				if (jobQueue.size() == 0) {
					newJob.setQueued(false);
					System.out.println(TAG+" queue is empty, starting job immediately");
				} else {
					System.out.println(TAG+" adding job to queue");
				}
				jobQueue.add(newJob);
				System.out.println(TAG+" queue length = "+jobQueue.size());
				break;

			// If this is a PUBLIC worker - execute job straight away
			case PUBLIC:
				newJob.setQueued(false);
				break;
		}

		newJob.startThread();
	}

	private static void stopJob(String jobId, Boolean deleteFiles) {
		Job job = getJob(jobId);

		if (job != null) {
			System.out.println(TAG+" stopping job "+jobId);
			job.stopJob(deleteFiles);

			if (type==WorkerType.PRIVATE) {
				popJobFromQueue(job);
			}
		}
	}

	private static void popJobFromQueue(Job finishedJob) {
		if (jobQueue.peek()==finishedJob) {
			jobQueue.poll();
		} else {
			jobQueue.remove(finishedJob);
		}

		if (jobQueue.size() > 0 ) {
			System.out.println(TAG+" starting next job in queue");
			System.out.println(TAG+" queue length = "+jobQueue.size());
			jobQueue.peek().setQueued(false);
			jobQueue.peek().startThread();
		} else {
			System.out.println(TAG+" job queue empty, no jobs to start");
		}
	}

	/*
	private static void sendFreeResourcesToMaster() {
		com.sun.management.OperatingSystemMXBean mxbean =
				(com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

		int avgCount = 5;
		double cpu_load = 0;
		long mem_free = 0;
		Sigar sigar = new Sigar();
		for (int i = 0; i<avgCount; i++) {
			try {

				cpu_load += sigar.getCpuPerc().getCombined();
				mem_free += sigar.getMem().getFree();
			} catch (SigarException e) {
				e.printStackTrace();
				avgCount --;
			}
			//cpu_load += mxbean.getProcessCpuLoad();
			//mem_free += mxbean.getFreePhysicalMemorySize();
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (avgCount != 0) {
			System.out.println(TAG + " cpu load = " + cpu_load / avgCount);
			System.out.println(TAG + " mem free = " + mem_free / avgCount);
			masterSocketServer.sendMessage("cpu_load " + cpu_load / avgCount);
			masterSocketServer.sendMessage("mem_free " + mem_free / avgCount);
			masterSocketServer.sendMessage("resources_check_finished");
		} else {
			masterSocketServer.sendMessage("resources_check_failed");
		}
	}
	*/


}