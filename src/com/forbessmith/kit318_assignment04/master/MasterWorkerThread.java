package com.forbessmith.kit318_assignment04.master;
import com.forbessmith.kit318_assignment04.socketcomms.*;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class MasterWorkerThread implements Runnable, SocketClient.MessageCallback {

	enum Status {
		INACTIVE, CREATING, INITIATING, CONNECTING, ACTIVE, UNREACHABLE, ERROR
	}

	public static final String  TAG = "<MasterWorkerThread>";

	private Status workerStatus;

	private String cloudServerId;
	private String cloudServerName;
	private String cloudServerStatus;
	private JCloudsNova jCloudsNova;
	private Boolean serverOnCloud;
	private int cloudServerNumVCpus;

	private Boolean workerCreated;
	private String workerHostname;
	private int workerPort;
	private SocketClient workerSocketClient;

	private Double cpuUsage = 1.0;
	private Calendar resourceCheckTime;
	private Calendar connectionTimeoutTime;

	private StatusChangeListener statusChangeListener;
	private ResourceUsageReceivedListener resourcesReceivedListener;

	private volatile Boolean running;
	private volatile Boolean waiting;

	MasterWorkerThread(int vCpuCount) {
		serverOnCloud = true;
		cloudServerNumVCpus = vCpuCount;
		workerStatus = Status.CREATING;
		workerCreated = false;
		workerHostname = "[new cloud server]";
		workerPort = -1;

		running = false;
		waiting = false;
	}

	MasterWorkerThread(String h, int p) {
		this (h, p, false);
	}
	MasterWorkerThread(String h, int p, Boolean onCloud) {
		workerCreated = true;
		workerHostname = h;
		workerPort = p;
		serverOnCloud = onCloud;

		workerStatus = Status.INITIATING;
		running = false;
		waiting = false;
	}

	// ------------------------------------------------------------------------
	// THREAD/RUNNABLE RELATED METHODS
	// ------------------------------------------------------------------------

	public void run() {
		running = true;

		if (getStatus() == Status.CREATING) {
			createNewWorker();

		} else
			setStatus(Status.INITIATING);


		while(running) {
			// Create socket and attempt to connect to worker - this methods WAITS for socket thread to finish/join
			connectToWorker();

			if (workerCreated) {
				try {
					System.out.println(getTag() + " waiting");
					waiting = true;
					synchronized (this) {
						while (waiting) {
							wait();
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		running = false;
		System.out.println(getTag()+" stopped");
	}

	public void stopRunning() {
		System.out.println(getTag()+" is stopping");
		if (workerSocketClient != null) {
			workerSocketClient.closeSocket();
			workerSocketClient.stopRunning();
		}
	}

	synchronized void notifyWorkerThread() {
		waiting = false;
		setStatus(Status.INITIATING);
		notify();
	}

	// ------------------------------------------------------------------------
	// WORKER METHODS
	// ------------------------------------------------------------------------
	private void createNewWorker() {
		System.out.println(TAG+ " creating new worker");
		jCloudsNova = new JCloudsNova();
		cloudServerName = UUID.randomUUID().toString();
		cloudServerId = jCloudsNova.createWorker(cloudServerName, cloudServerNumVCpus);
		cloudServerNumVCpus = jCloudsNova.getServerNumVCpus();

		System.out.println(TAG+ " worker created - waiting for initialisation");
		cloudServerStatus = "BUILD";
		while (cloudServerStatus.equals("BUILD")) {
			try {
				System.out.println(TAG + " waiting for server - current server status: " + cloudServerStatus);
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				cloudServerStatus = jCloudsNova.getServerStatus();
			} catch (Exception e) {
				cloudServerStatus = "FAILED";
			}
		}

		if (cloudServerStatus.equals("ACTIVE")) {
			workerHostname = jCloudsNova.getServerIp();
			workerPort = 1234;
			System.out.println(TAG + " server successfully created! - current server status: " + cloudServerStatus);
			System.out.println(getTag() + " server hostname = " + workerHostname);
			System.out.println(getTag() + " server port = " + workerPort);
			//setStatus(Status.INITIATING);
		} else {
			System.out.println(TAG+" could not create new cloud server - status = "+cloudServerStatus);
			Master.createWorkerFailed(this);
			running = false;
		}
	}

	private void connectToWorker() {
		workerSocketClient = new SocketClient(getTag() + " [WorkerClient]", workerHostname, workerPort);
		workerSocketClient.setConnectionRetryDelayMs(50);
		workerSocketClient.setConnectionRetries(5);

		workerSocketClient.setOnMessageReceivedListener(this);

		Thread workerSocketThread = new Thread(workerSocketClient);

		workerSocketClient.setOnStatusChangeListener((socketCommunication, currentStatus) -> {
			switch (currentStatus) {
				case CONNECTING:
					setStatus(Status.CONNECTING);
					break;
				case CONNECTED:
					setStatus(Status.ACTIVE);
					if (!workerCreated) {
						workerCreated = true;
						Master.newWorkerCreated();
					}
					break;
				case DISCONNECTED:
					setStatus(Status.INACTIVE);
					break;
				case TIMEOUT:
					resetLastConnectionTimeoutTime();
					setStatus(Status.UNREACHABLE);
					break;
			}
		});

		workerSocketThread.start();

		try {
			workerSocketThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (jCloudsNova != null) {
			try {
				jCloudsNova.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		setStatus(Status.INACTIVE);
	}

	// Worker Socket message received
	@Override
	public void newMessageReceived(String message) {
		System.out.println(workerSocketClient.getTag() + " received "+message);
		String messageCommand;
		try
		{
			messageCommand = message.split(" ")[0];
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			messageCommand = "";
		}

		switch (messageCommand) {
			case "ready":
				break;

			case "result_listening_port":
				try {
					String jobId = message.split(" ")[1];
					int jobPort = Integer.parseInt(message.split(" ")[2]);
					System.out.println(getTag()+" received job port number "+ jobId+" port="+jobPort);
					Master.jobSocketPortReceived(jobId, jobPort);
					resourceCheckTime = null;
				} catch (NumberFormatException e) {
					System.out.println(getTag()+" Couldn't retrieve WorkerProcess workerPort - trying again");
					workerSocketClient.sendMessage("result_listening_port resend");
				}
				break;

			case "work_process_failed":
				System.out.println(getTag()+" work_process_failed job= "+message.split(" ")[1]);
				break;

			case "cpu_load":
				cpuUsage = Double.parseDouble(message.split(" ")[1]);
				System.out.println(getTag()+" cpu usage - "+ cpuUsage);
				break;

			case "mem_free":
				//memFree = Long.parseLong(message.split(" ")[1]);
				//System.out.println(getTag()+" free memory - "+ memFree);
				break;

			case "resources_check_finished":
				resourceCheckTime = Calendar.getInstance();
				System.out.println(getTag()+" finished receiving resource utilisation");
				if (resourcesReceivedListener != null)
					resourcesReceivedListener.onResourcesReceived(this);
				break;

		}
	}

	void getCpuUsageFromWorker() {
		if (!isLastResourceUsageCheckRecent()) {
			// If non-cloud worker (with Sigar installed)
			if (!serverOnCloud) {
				if (workerSocketClient == null || !workerSocketClient.isRunning())
					connectToWorker();
				System.out.println(getTag() + " asking worker for resource usage");
				workerSocketClient.sendMessage("send_resource_usage");
			}
			// if cloud worker
			else {
				System.out.println(TAG+" calculating pseudo worker resource usage");
				generatePseudoCpuUsage();
			}
		} else {
			System.out.println(getTag() + " already have recent resource usage");
			if (resourcesReceivedListener != null)
				resourcesReceivedListener.onResourcesReceived(this);
		}
	}

	private void generatePseudoCpuUsage() {
		//cpuUsage = Double.parseDouble(message.split(" ")[1]);
		Iterator<Map.Entry<String, FreqCountJob>> jobIterator = Master.getActiveJobs().entrySet().iterator();
		int totalKWords = 0;
		while (jobIterator.hasNext()) {
			FreqCountJob currrentJob = jobIterator.next().getValue();
			if (currrentJob.getStatus() == FreqCountJob.Status.ACTIVE && currrentJob.getWorker() == this)
				totalKWords += currrentJob.getkFreqWords();
		}
		// cpu usage ~ 10,000,000 k words per virtual cpu
		cpuUsage = (double) (totalKWords * cloudServerNumVCpus) /10000000;
		if (cpuUsage > 1)
			cpuUsage = 1.0;
		System.out.println(getTag()+" [pseudo] cpu usage - "+ cpuUsage);
		resourceCheckTime = Calendar.getInstance();
		System.out.println(getTag()+" finished receiving resource utilisation");
		if (resourcesReceivedListener != null)
			resourcesReceivedListener.onResourcesReceived(this);
	}


	// ------------------------------------------------------------------------
	// JOB METHODS
	// ------------------------------------------------------------------------

	/**
	 *  Creates a new job - sends command to worker
	 */
	synchronized Boolean createNewJob(FreqCountJob job) {
		System.out.println(getTag()+" Create new job "+job.getId()+" "+job.getkFreqWords()+" "+job.getStreamHostname()+" "+job.getStreamPort());
		workerSocketClient.sendMessage("new_worker_process "
				+job.getId()+" "+job.getkFreqWords()+" "+job.getStreamHostname()+" "+job.getStreamPort());
		return true;
	}

	/**
	 *  Connects to job worker process - asks for results
	 *  When results are received the connection is killed
	 */
	void getResultsOnce(FreqCountJob job) {
		if (job.getStatus() == FreqCountJob.Status.ACTIVE) {
			job.setResults("Results from "+DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(Calendar.getInstance().getTime()));

			job.setCurrentlyRetrievingResults(true);
			int jobPort = job.getWorkerProcessPort();
			final SocketClient workerProcessSocketClient = new SocketClient(getTag()+" [WorkerProcessClient]",workerHostname, jobPort);

			workerProcessSocketClient.setOnMessageReceivedListener( message -> {
				if (message.equals("results_finished")) {
					job.addResults("END OF RESULTS");
					job.setCurrentlyRetrievingResults(false);
					System.out.println(TAG+" job "+job.getId()+" results \n"+job.getResults());
					workerProcessSocketClient.stopRunning();
					workerProcessSocketClient.closeSocket();
				} else {
					job.addResults(message);
				}
			});

			new Thread(workerProcessSocketClient).start();
			workerProcessSocketClient.sendMessage("results_please");

			job.setOnStatusChangeListener((job1, currentStatus) -> {
				if (!currentStatus.equals(FreqCountJob.Status.ACTIVE)) {
					workerProcessSocketClient.stopRunning();
					workerProcessSocketClient.closeSocket();
					job.resetResults();
				}
			});
		} else {
			System.out.println(getTag()+" job "+job.getId()+" is not active");
		}
	}

	/**
	 *  Connects to job worker process - asks to finish job
	 *  When job is finished the connection is killed
	 */
	void finishJob(FreqCountJob job) {
		if (job.getStatus() == FreqCountJob.Status.ACTIVE) {
			int jobPort = job.getWorkerProcessPort();
			final SocketClient workerProcessSocketClient = new SocketClient(getTag()+" [WorkerProcessClient]",workerHostname, jobPort);

			workerProcessSocketClient.setOnMessageReceivedListener( message -> {
				System.out.println(getTag() + " received " + message);
				workerProcessSocketClient.stopRunning();
				workerProcessSocketClient.closeSocket();
				job.finishJob();
			});

			new Thread(workerProcessSocketClient).start();
			workerProcessSocketClient.sendMessage("finish_job");
		} else {
			System.out.println(getTag()+" job "+job.getId()+" is not active");
		}
	}

	// ------------------------------------------------------------------------
	// GETTERS/SETTERS & OTHER SIMPLE PROPERTY METHODS
	// ------------------------------------------------------------------------

	synchronized void setStatus(Status status) {
		workerStatus = status;
		if (statusChangeListener != null)
			statusChangeListener.onJobStatusChanged(this, status);
	}
	synchronized Status getStatus() {
		return workerStatus;
	}

	String getWorkerHost() {
		return workerHostname +":"+ workerPort;
	}

	Boolean isConnected() {return workerStatus==Status.ACTIVE;}

	Boolean isConnecting() {return workerStatus==Status.CONNECTING||workerStatus==Status.INITIATING;}

	Boolean isRunning() {return running;}

	String getTag() {
		return TAG+" "+getWorkerHost();
	}

	Double getCpuUsage() {
		if (cpuUsage ==null) {
			return 1.0;
		}
		return cpuUsage;
	}

	Boolean isLastResourceUsageCheckRecent() {
		if (resourceCheckTime == null)
			return false;

		Calendar resourceCheckTimeCutoff = Calendar.getInstance();
		resourceCheckTimeCutoff.add(Calendar.HOUR, -1);
		return resourceCheckTime.after(resourceCheckTimeCutoff);
	}

	void resetLastConnectionTimeoutTime() {
		connectionTimeoutTime = Calendar.getInstance();
	}
	Boolean isLastConnectionTimeoutRecent() {
		if (connectionTimeoutTime == null)
			return false;

		Calendar connectionTimeoutTimeCutoff = Calendar.getInstance();
		connectionTimeoutTimeCutoff.add(Calendar.HOUR, -1);
		return connectionTimeoutTime.after(connectionTimeoutTimeCutoff);
	}

	// ------------------------------------------------------------------------
	// INTERFACES & INTERFACE SETTER METHODS
	// ------------------------------------------------------------------------

	void setOnStatusChangeListener(StatusChangeListener listener) {
		statusChangeListener = listener;
	}

	interface StatusChangeListener {
		void onJobStatusChanged(MasterWorkerThread worker, Status currentStatus) ;
	}

	void setOnResourcesReceivedListener(ResourceUsageReceivedListener listener) {
		resourcesReceivedListener = listener;
	}

	interface ResourceUsageReceivedListener {
		void onResourcesReceived(MasterWorkerThread worker) ;
	}
}