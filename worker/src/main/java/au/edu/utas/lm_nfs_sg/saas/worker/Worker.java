package au.edu.utas.lm_nfs_sg.saas.worker;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketClient;
import au.edu.utas.lm_nfs_sg.saas.comms.SocketServer1To1;
import jdk.nashorn.internal.scripts.JO;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class Worker {
	static final String TAG = "<Worker>";

	private static SocketServer1To1 masterSocketServer;
	private static MasterRestClient masterRestClient;

	public static String masterHostname;

	private static Map<String, JobControllerThread> jobs = Collections.synchronizedMap(new HashMap<String, JobControllerThread>());


	private Worker(int p) {

	}

	public static void main(String[] args) {
		if (args.length >= 2) {
			masterHostname = args[0];

			int port = 0;
			try {
				port = Integer.parseInt(args[1]);
				// If invalid port number is provided - exit program
				if (port < 1024 || port > 65535) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				System.out.println("Argument '" + args[1] + "' is an invalid port number - expecting an integer between 1024 and 65535");
				System.exit(-1);
			}


			MasterRestClient.setMasterHostname(masterHostname);

			masterRestClient = new MasterRestClient("WorkerMain");

			masterSocketServer = new SocketServer1To1(TAG+" MasterSocketServer",port);
			masterSocketServer.setOnMessageReceivedListener(message -> handleSocketMessageFromMaster(message));

			Thread masterSocketThread = new Thread(masterSocketServer);
			masterSocketThread.start();
			masterSocketServer.sendMessage("ready");

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

	static JobControllerThread getJobController(String jobId) {
		if (jobs.containsKey(jobId)) {
			return jobs.get(jobId);
		} else {
			System.out.println(TAG+" couldn't find "+jobId);
		}
		return null;
	}

	private static void launchNewJob(String jobId) {
		System.out.println(TAG+" starting job "+jobId);

		JobControllerThread newJobThread = new JobControllerThread(jobId,false);

		jobs.put(jobId, newJobThread);
		startJobControllerThread(newJobThread);
	}

	private static void stopJob(String jobId, Boolean deleteFiles) {
		JobControllerThread job = getJobController(jobId);

		if (job != null) {
			System.out.println(TAG+" stopping job "+jobId);
			job.stopJop(deleteFiles);
		}
	}

	private static Boolean startJobControllerThread(JobControllerThread jobControllerThread) {
		if (!jobControllerThread.isRunning()) {
			new Thread(jobControllerThread).start();
		}
		else if (!jobControllerThread.isWaiting()) {
			jobControllerThread.notifyJobControllerThread();
		}
		return true;

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