package au.edu.utas.lm_nfs_sg.saas.master;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketClient;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;

import java.io.IOException;
import java.util.Calendar;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

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
	private Flavor instanceFlavour;

	private Boolean workerCreated;
	private String workerHostname;
	private int workerPort;
	private SocketClient workerSocketClient;

	private Boolean sharedWorker = false;
	private Queue<Job> jobQueue;

	private Boolean available = true;
	private int numJobsRunning = 0;

	private Calendar connectionTimeoutTime;
	private Calendar createWorkerStartTime;
	private Calendar createWorkerEndTime;

	private StatusChangeListener statusChangeListener;

	private volatile Boolean running;
	private volatile Boolean waiting;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	// Create new worker/cloud instance
	MasterWorkerThread(Flavor instanceFlavour, Boolean isSharedWorker) {
		workerHostname = "[unknown]";
		workerPort = -1;

		workerCreated = false;
		serverOnCloud = true;

		this.instanceFlavour = instanceFlavour;

		sharedWorker = isSharedWorker;

		workerStatus = Status.CREATING;

		init();
	}

	// Already existing worker
	MasterWorkerThread(String h, int p, Boolean onCloud, Boolean isSharedWorker) {
		workerHostname = h;
		workerPort = p;

		workerCreated = true;
		serverOnCloud = onCloud;
		sharedWorker = isSharedWorker;

		workerStatus = Status.INITIATING;

		init();
	}

	private void init() {
		running = false;
		waiting = false;

		if(!sharedWorker) {
			jobQueue = new ConcurrentLinkedQueue<Job>();
		}
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

	public Boolean startThread() {
		if (getStatus() != MasterWorkerThread.Status.CREATING) {
			if (!isRunning())
				new Thread(this).start();
			else if (!isConnected())
				notifyWorkerThread();

			while (isConnecting()) {
				try {
					if (isLastConnectionTimeoutRecent())
						break;
					Thread.sleep(500);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		return isConnected();

	}

	// ------------------------------------------------------------------------
	// WORKER METHODS
	// ------------------------------------------------------------------------
	private void createNewWorker() {
		createWorkerStartTime = Calendar.getInstance();

		System.out.println(TAG+ " creating new worker");
		jCloudsNova = new JCloudsNova();

		cloudServerName = UUID.randomUUID().toString();
		cloudServerId = jCloudsNova.createWorker(cloudServerName, sharedWorker);

		// Assign instance flavour again - just in case it has been changed in process of creation
		instanceFlavour = jCloudsNova.getInstanceFlavour();

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
			workerPort = 8081;
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
					if (!workerCreated) {
						createWorkerEndTime = Calendar.getInstance();
						workerCreated = true;
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

		addJobToQueue(job);
	}

	/**
	 *  Connects to worker - asks to delete job
	 */
	void deleteJob(Job job) {
		job.setStatus(Job.Status.STOPPING);
		workerSocketClient.sendMessage("delete_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  Connects to worker - asks to finish job
	 */
	void stopJob(Job job) {
		job.setStatus(Job.Status.STOPPING);
		workerSocketClient.sendMessage("stop_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  When a job finishes on worker - remove from Queue
	 */
	void jobFinished(Job job) {
		removeJobFromQueue(job);
	}

	// ------------------------------------------------------------------------
	// JOB QUEUE METHODS
	// ------------------------------------------------------------------------

	synchronized void addJobToQueue(Job job) {
		if(!sharedWorker) {
			jobQueue.add(job);
		}
	}

	synchronized void removeJobFromQueue(Job job) {
		if (!sharedWorker) {
			if (jobQueue.contains(job))
				jobQueue.remove(job);
		}
	}

	synchronized Calendar estimateQueueCompletionCalendar() {
		Calendar returnCal = Calendar.getInstance();
		returnCal.add(Calendar.MILLISECOND, estimateQueueCompletionTimeInMs().intValue());

		return returnCal;
	}

	synchronized Long estimateQueueCompletionTimeInMs() {
		Long time = 0L;
		if (!sharedWorker) {
			for (Job job : jobQueue) {
				time += job.estimateExecutionTimeInMs(jCloudsNova.getInstanceFlavour());
				if (job.getStatus() == Job.Status.RUNNING) {
					time -= job.getUsedCpuTimeInMs();
				}
			}
		}

		System.out.println(getTag()+" Estimated time for queue completion is "+time.toString()+" ms");

		return time;
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

	Boolean isSharedWorker() {return sharedWorker;}

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
		connectionTimeoutTimeCutoff.add(Calendar.MINUTE, -10);
		return connectionTimeoutTime.after(connectionTimeoutTimeCutoff);
	}

	public Flavor getInstanceFlavour() {
		return instanceFlavour;
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