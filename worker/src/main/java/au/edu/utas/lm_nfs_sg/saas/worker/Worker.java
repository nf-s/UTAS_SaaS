package au.edu.utas.lm_nfs_sg.saas.worker;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketServer1To1;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Collections.*;

public final class Worker {
	static final String TAG = "<Worker>";

	enum Status {
		INITIATING, ACTIVE, FAILURE, MIGRATING
	}

	private static String id;

	private static SocketServer1To1 masterSocketServer;
	private static MasterRestClient masterRestClient;

	private static String masterHostname;

	private static Map<String, Job> jobs;
	private static Queue<Job> jobQueue;

	private static Boolean sharedWorker;
	private static Status status;


	/**
	 *  ARGUMENTS (expects 4):
	 *  1 - Worker id (string)
	 *  2 - Master hostname (string)
	 *  3 - Master socket port (integer between 1024 and 65535)
	 *  4 - Shared worker (boolean - true = shared worker, false = unshared worker)
	 *
	 */
	public static void main(String[] args) {
		if (args.length >= 4) {
			status = Status.INITIATING;

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

			sharedWorker = Boolean.parseBoolean(args[3]);

			MasterRestClient.setMasterHostname(masterHostname);
			masterRestClient = new MasterRestClient("WorkerMain");

			masterSocketServer = new SocketServer1To1(TAG+" MasterSocketServer",port);
			masterSocketServer.setOnMessageReceivedListener(message -> handleSocketMessageFromMaster(message));

			Thread masterSocketThread = new Thread(masterSocketServer);
			masterSocketThread.start();

			jobs = synchronizedMap(new HashMap<String, Job>());
			if(!sharedWorker) {
				jobQueue = new ConcurrentLinkedQueue<Job>();
			}

			setStatus(Status.ACTIVE);

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
				//runProcessFromString(mess);
				launchNewJob(mess.split(" ")[1]);
				break;

			case "stop_job":
				//runProcessFromString(mess);
				stopJob(mess.split(" ")[1], false);
				break;

			case "delete_job":
				//runProcessFromString(mess);
				stopJob(mess.split(" ")[1], true);
				break;
		}
	}

	private static void setStatus(Status newStatus) {
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

		Job newJobThread = new Job(jobId,!sharedWorker);

		jobs.put(jobId, newJobThread);
		if(!sharedWorker) {
			newJobThread.setOnStatusChangeListener((job, currentStatus) -> {
				if (currentStatus == Job.Status.FINISHING || currentStatus == Job.Status.ERROR) {
					popJobFromQueue(job);
					newJobThread.setOnStatusChangeListener(null);
				}
			});
			// If this job queue is empty - execute job straight away
			if (jobQueue.size() == 0) {
				newJobThread.setQueued(false);
				System.out.println(TAG+" queue is empty, starting job immediately");
			} else {
				System.out.println(TAG+" adding job to queue");
			}
			jobQueue.add(newJobThread);
			System.out.println(TAG+" queue length = "+jobQueue.size());
		}
		newJobThread.startThread();
	}

	private static void stopJob(String jobId, Boolean deleteFiles) {
		Job job = getJob(jobId);

		if (job != null) {
			System.out.println(TAG+" stopping job "+jobId);
			job.stopJob(deleteFiles);

			if (!sharedWorker) {
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