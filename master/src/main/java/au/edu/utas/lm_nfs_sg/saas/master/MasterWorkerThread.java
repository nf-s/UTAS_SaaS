package au.edu.utas.lm_nfs_sg.saas.master;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketClient;

import java.io.IOException;
import java.util.Calendar;
import java.util.UUID;

public class MasterWorkerThread implements Runnable, SocketClient.MessageCallback {

	enum Status {
		INACTIVE, CREATING, INITIATING, CONNECTING, ACTIVE, UNREACHABLE, ERROR, FAILURE
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

	private Boolean available = true;
	private int numJobsRunning = 0;

	private Calendar connectionTimeoutTime;

	private StatusChangeListener statusChangeListener;

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
			setStatus(Status.FAILURE);
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
					workerCreated = true;
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
				System.out.println(getTag()+" Worker Available");
				break;
		}
	}

	// ------------------------------------------------------------------------
	// JOB METHODS
	// ------------------------------------------------------------------------

	/**
	 *  Creates a new job - sends command to worker
	 */
	synchronized void assignJob(Job job) {
		System.out.println(getTag()+" Assigning job "+job.getId());
		workerSocketClient.sendMessage("new_job "+job.getId());
		//available=true;

		numJobsRunning ++;
	}

	/**
	 *  Connects to worker - asks to delete job
	 */
	void deleteJob(Job job) {
		job.setStatus(Job.Status.STOPPING);
		workerSocketClient.sendMessage("delete_job "+job.getId());
	}

	/**
	 *  Connects to worker - asks to finish job
	 */
	void stopJob(Job job) {
		job.setStatus(Job.Status.STOPPING);
		workerSocketClient.sendMessage("stop_job "+job.getId());
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

	Boolean isAvailable() {
		if (numJobsRunning > 10) {
			available=false;
		}
		return available;
	}

	int getNumJobs() {
		return numJobsRunning;
	}

	String getTag() {
		return TAG+" "+getWorkerHost();
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
}