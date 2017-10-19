package au.edu.utas.lm_nfs_sg.saas.master;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketClient;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;

import java.io.IOException;
import java.util.Calendar;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Worker implements Runnable {

	enum Status {
		INACTIVE, CREATING, CREATED, CREATE_FAIL, INITIATING, ACTIVE, UNREACHABLE, MIGRATING, ERROR, FAILURE
	}

	private static final String  TAG = "<Worker>";
	private static final int SOCKET_DEFAULT_CONN_RETRY_DELAY_MS = 50;
	private static final int SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS = 10;
	private static final int SOCKET_CREATEWORKER_CONN_RETRY_DELAY_MS = 500;

	private JCloudsNova jCloudsNova;
	private Flavor instanceFlavour;

	private String id;
	private String hostname;
	private int workerPort;

	private Thread workerSocketThread;
	private SocketClient workerSocketClient;

	private Boolean sharedWorker = false;
	private Queue<Job> jobQueue;

	private Boolean available = true;

	private Calendar connectionTimeoutTime;

	private Status status;
	private StatusChangeListener statusChangeListener;

	private volatile Boolean running=false;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	// Create new worker/cloud instance
	Worker(Flavor instanceFlavour, Boolean isSharedWorker) {
		hostname = "unknown.hostname";
		workerPort = 0;

		this.instanceFlavour = instanceFlavour;

		sharedWorker = isSharedWorker;

		// Set initial status - this determines that a worker needs to be created
		status = Status.CREATING;

		id = UUID.randomUUID().toString();

		init();
	}

	// Already existing worker - NEED TO UPDATE
	Worker(String id, String h, int p, Boolean isSharedWorker) {
		hostname = h;
		workerPort = p;

		sharedWorker = isSharedWorker;

		// Set initial status
		status = Status.INITIATING;

		this.id =id;

		init();
	}

	private void init() {
		jobQueue = new ConcurrentLinkedQueue<Job>();
	}

	// ------------------------------------------------------------------------
	// THREAD/RUNNABLE RELATED METHODS
	// ------------------------------------------------------------------------

	public void run() {
		running = true;
		if (getStatus() == Status.CREATING) {
			createNewWorker();

			if (getStatus() != Status.CREATE_FAIL) {
				// Create socket and attempt to connect to worker
				startWorkerSocketThread(true);
			}

		} else {
			startWorkerSocketThread(false);
		}

		if (workerSocketThread != null) {
			try {
				workerSocketThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			setStatus(Status.INACTIVE);
		}

		if (jCloudsNova != null) {
			try {
				jCloudsNova.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		running = false;
		if (Master.DEBUG)
			System.out.println(getTag()+" stopped");
	}

	public void stopRunning() {
		if (Master.DEBUG)
			System.out.println(getTag()+" is stopping");
		if (workerSocketClient != null) {
			workerSocketClient.closeSocket();
			workerSocketClient.stopRunning();
		}
	}

	public Boolean connectToWorker() {
		if (getStatus() != Worker.Status.CREATING) {
			if (!isRunning())
				new Thread(this).start();

			while (!isConnected()) {
				try {
					// If connecting timeout occurred within the last 10 minutes - break from loop
					if (isLastConnectionTimeoutRecent())
						break;

					// Sleep for socket to exhaust all connection attempts (with delay)
					Thread.sleep(SOCKET_DEFAULT_CONN_RETRY_DELAY_MS * SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS);

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

	/**
	 *  Attempt to create a new instance with JClouds
	 *
	 *  NOTE: When a worker has been successfully created and it is ready to accept jobs
	 *  	  it will update its status through REST API (through WorkerResource.class)
	 */
	private void createNewWorker() {
		if (Master.DEBUG)
			System.out.println(TAG+ " creating new worker");
		jCloudsNova = new JCloudsNova();

		if (jCloudsNova.createWorker(id, sharedWorker)) {
			// Successfully launched instance - set hostname

			hostname = jCloudsNova.getInstanceHostname();
			workerPort = 8081;

			setStatus(Status.CREATED);
		} else {
			if (Master.DEBUG)
				System.out.println(TAG+" could not create new cloud server - status = ");
			setStatus(Status.CREATE_FAIL);
			stopRunning();
		}
	}

	/**
	 *  Starts a new SocketThread which attempts to connect to the worker
	 *
	 *  + If a worker has just been created - the socket will attempt to connect to the worker indefinitely
	 *  + If not - the socket will use the default settings (SOCKET_CREATEWORKER_CONN_RETRY_DELAY_MS and SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS)
	 *
	 */
	private void startWorkerSocketThread(Boolean connectingToNewCreatedWorker) {
		if (workerSocketThread == null) {
			workerSocketClient = new SocketClient(getTag() + " [WorkerClient]", hostname, workerPort);

			if (connectingToNewCreatedWorker) {
				workerSocketClient.setConnectionRetryDelayMs(SOCKET_CREATEWORKER_CONN_RETRY_DELAY_MS);
				workerSocketClient.setConnectionRetries(0); // 0 - indicates unlimited retries
			} else {
				workerSocketClient.setConnectionRetryDelayMs(SOCKET_DEFAULT_CONN_RETRY_DELAY_MS);
				workerSocketClient.setConnectionRetries(SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS);
			}

			workerSocketClient.setOnMessageReceivedListener((SocketClient.MessageCallback) this::receivedMessageFromWorker);

			workerSocketClient.setOnStatusChangeListener((socketCommunication, currentStatus) -> {
				switch (currentStatus) {
					case CONNECTED:
						// If connected to a newly created worker - reset connection retry parameters to default
						if (connectingToNewCreatedWorker) {
							jCloudsNova.launchedSuccessfully();

							workerSocketClient.setConnectionRetryDelayMs(SOCKET_DEFAULT_CONN_RETRY_DELAY_MS);
							workerSocketClient.setConnectionRetries(SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS);
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

			workerSocketThread = new Thread(workerSocketClient);
			workerSocketThread.start();
		}
		else if (!workerSocketThread.isAlive()) {
			workerSocketThread = new Thread(workerSocketClient);
			workerSocketThread.start();
		}
	}

	private void receivedMessageFromWorker(String message) {
		if (Master.DEBUG)
			System.out.println(workerSocketClient.getTag() + " received "+message);
	}

	// ------------------------------------------------------------------------
	// JOB METHODS
	// ------------------------------------------------------------------------

	/**
	 *  Creates a new job - sends command to worker
	 */
	void assignJob(Job job) {
		if (Master.DEBUG)
			System.out.println(getTag()+" Assigning job "+job.getId());
		workerSocketClient.sendMessage("new_job "+job.getId());
		//available=true;

		addJobToQueue(job);
	}

	/**
	 *  Connects to worker - asks to delete job
	 */
	void deleteJob(Job job) {
		job.setStatus(Job.Status.DELETING_ON_MASTER);
		workerSocketClient.sendMessage("delete_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  Connects to worker - asks to finish job
	 */
	void stopJob(Job job) {
		job.setStatus(Job.Status.STOPPING_ON_MASTER);
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

	private synchronized void addJobToQueue(Job job) {
			jobQueue.add(job);
	}

	private synchronized void removeJobFromQueue(Job job) {
		if (jobQueue.contains(job))
			jobQueue.remove(job);
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

		if (Master.DEBUG)
			System.out.println(getTag()+" Estimated time for queue completion is "+time.toString()+" ms");

		return time;
	}

	synchronized Boolean isAvailable() {
		if (jobQueue.size() > 10) {
			available=false;
		}
		return available;
	}

	synchronized int getNumJobs() {
		return jobQueue.size();
	}

	// ------------------------------------------------------------------------
	// GETTERS/SETTERS & OTHER SIMPLE PROPERTY METHODS
	// ------------------------------------------------------------------------
	String getId() {return id;}
	String getHostname() {return hostname;}

	synchronized void setStatus(Status newStatus) {
		if (Master.DEBUG)
			System.out.printf("%s Updated status: %s%n", getTag(), newStatus.toString());

		switch (newStatus) {
			case ACTIVE:
				break;
		}

		if (statusChangeListener != null)
			statusChangeListener.onJobStatusChanged(this, newStatus);

		status = newStatus;
	}
	synchronized Status getStatus() {
		return status;
	}

	String getHostWithPortString() {
		return hostname +":"+ workerPort;
	}

	Boolean isSharedWorker() {return sharedWorker;}

	Boolean isConnected() {return status ==Status.ACTIVE;}

	Boolean isRunning() {return running;}

	String getTag() {
		return String.format("%s [%s]", TAG, getHostWithPortString());
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
		void onJobStatusChanged(Worker worker, Status currentStatus) ;
	}
}