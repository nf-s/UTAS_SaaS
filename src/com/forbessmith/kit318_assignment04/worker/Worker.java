package com.forbessmith.kit318_assignment04.worker;

import com.forbessmith.kit318_assignment04.socketcomms.SocketClient;
import com.forbessmith.kit318_assignment04.socketcomms.SocketServer1To1;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class Worker implements SocketServer1To1.MessageCallback {
	static final String TAG = "Worker";

	static private SocketServer1To1 masterSocketServer;

	static private WorkerProcessMonitorThread workerProcessMonitorThread;

	private Worker(int p) {
		workerProcessMonitorThread = new WorkerProcessMonitorThread();
		new Thread(workerProcessMonitorThread).start();

		masterSocketServer = new SocketServer1To1(TAG+" MasterServer",p);
		masterSocketServer.setOnMessageReceivedListener(this);
		Thread masterSocketThread = new Thread(masterSocketServer);
		masterSocketThread.start();
		masterSocketServer.sendMessage("ready");

		try {
			masterSocketThread.join();
			workerProcessMonitorThread.stopThread();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length >= 1) {
			int port = 0;
			int num_freq_words = 0;
			try {
				port = Integer.parseInt(args[0]);
				// If invalid port number is provided - exit program
				if (port < 1024 || port > 65535) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				System.out.println("Argument '" + args[0] + "' is an invalid port number - expecting an integer between 1024 and 65535");
				System.exit(-1);
			}

			Worker worker = new Worker(port);

		}// If less than 3 arguments are given when executing the program
		else {
			System.out.println("Incorrect number of arguments given");
			System.exit(-1);
		}
	}

	@Override
	public void newMessageReceived(String mess) {
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
			case "new_worker_process":
				runProcessFromString(mess);
				break;
			case "remove_worker_process":
				if (!workerProcessMonitorThread.removeProcess(mess)) {
					try {
						String jobId = mess.split(" ")[1];
						masterSocketServer.sendMessage("error remove_worker_process id "+jobId);
					} catch (Exception e) {
						masterSocketServer.sendMessage("error remove_worker_process incorrect_args");
						e.printStackTrace();
					}
				}
				break;
			/*case "send_resource_usage":
				sendFreeResourcesToMaster();*/
		}

	}

	static void runProcessFromString(String s) {
		try
		{
			String jobId = s.split(" ")[1];
			int freqCountK = Integer.parseInt(s.split(" ")[2]);
			String streamServerHostname = s.split(" ")[3];
			int streamServerPort = Integer.parseInt(s.split(" ")[4]);

			Process p = runProcess(jobId, freqCountK, streamServerHostname, streamServerPort);

			if (p!= null) {
				workerProcessMonitorThread.addProcess(s, p);
			}
		}
		catch (ArrayIndexOutOfBoundsException | NumberFormatException e)
		{
			masterSocketServer.sendMessage("error new_worker_process incorrect_args");
			e.printStackTrace();
		} catch (Exception e) {
			masterSocketServer.sendMessage("error new_worker_process could_not_start_worker_process");
			e.printStackTrace();
		}
	}

	private static Process runProcess(String jobId, int freqCountK, String streamServerHostname, int streamServerPort) throws Exception {
		Path currentAbsolutePath = Paths.get("").toAbsolutePath();
		String command = "java com.forbessmith.kit318_assignment04.worker.WorkerProcess "+jobId+" "+freqCountK+" "+streamServerHostname+" "+streamServerPort;
		System.out.println("RUN NEW [WorkerProcess] - "+command);
		Process pro = Runtime.getRuntime().exec(command);

		handleWorkerProcessStdOut("[WorkerProcess] stdout:", pro.getInputStream());
		//handleWorkerProcessStdOut("[WorkerProcess] stderr:", pro.getErrorStream());

		if (pro.waitFor(500, TimeUnit.MILLISECONDS)) {
			System.out.println("[WorkerProcess] exitValue() " + pro.exitValue());
			masterSocketServer.sendMessage("error work_process_failed "+jobId);
			return null;
		} else {
			System.out.println("[WorkerProcess] Stopped listening to job "+jobId+" - process is still running");
			return pro;
		}
	}

	private static void handleWorkerProcessStdOut(String name, InputStream ins) throws Exception {
		Boolean listenToProcess = true;
		String line = null;
		BufferedReader in = new BufferedReader(	new InputStreamReader(ins));
		while ((line = in.readLine()) != null) {
			String lineSplit;
			// If message has any words - try to extract possible commands
			try
			{
				lineSplit = line.split(" ")[0];
			}
			catch (ArrayIndexOutOfBoundsException e)
			{
				lineSplit = "";
			}

			if (lineSplit.equals("result_listening_port")) {
				sendWorkerProcessPortToMaster(line);

				break;
			} else {
				System.out.println(name + " " + line);
			}
		}
	}

	private static void sendWorkerProcessPortToMaster(String message) {
		if (masterSocketServer.isConnected())
			masterSocketServer.sendMessage(message);
		else {
			SocketClient masterSocketClient = new SocketClient(TAG+" masterClient", "localhost", 1025);
			masterSocketClient.setOnMessageReceivedListener(message1 -> {
				if (message1.equals("thanks")) {
					masterSocketClient.stopRunning();
					masterSocketClient.closeSocket();
				}
			} );
			new Thread(masterSocketClient).start();

			masterSocketClient.sendMessage(message);
		}
	}

	public static int getpid(Process pro){
		if(pro.getClass().getName().equals("java.lang.UNIXProcess")) {
	   /* get the PID on unix/linux systems */
			try {
				Field f = pro.getClass().getDeclaredField("pid");
				f.setAccessible(true);
				int pid = f.getInt(pro);
				return pid;
			} catch (Throwable e) {
			}

		}
		return 0;
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