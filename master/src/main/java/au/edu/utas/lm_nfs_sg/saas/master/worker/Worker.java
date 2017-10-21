package au.edu.utas.lm_nfs_sg.saas.master.worker;

import au.edu.lm_nf_sg.saas.common.job.JobStatus;
import au.edu.lm_nf_sg.saas.common.worker.WorkerStatus;
import au.edu.lm_nf_sg.saas.common.worker.WorkerType;
import au.edu.utas.lm_nfs_sg.saas.comms.SocketClient;
import au.edu.utas.lm_nfs_sg.saas.master.Master;
import au.edu.utas.lm_nfs_sg.saas.master.job.Job;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;

import java.io.IOException;
import java.util.Calendar;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Worker implements Runnable {

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

	private WorkerType type = WorkerType.PUBLIC;
	private Queue<Job> jobQueue;
	private Queue<Job> unassignedJobQueue;

	private Boolean available = true;

	private Calendar connectionTimeoutTime;

	private WorkerStatus status;
	private StatusChangeListener statusChangeListener;

	private volatile Boolean running=false;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	// Create new worker/cloud instance
	public Worker(Flavor instanceFlavour, WorkerType workerType) {
		hostname = "unknown.hostname";
		workerPort = 0;

		this.instanceFlavour = instanceFlavour;

		type = workerType;

		// Set initial status - this determines that a worker needs to be created
		status = WorkerStatus.CREATING;

		id = UUID.randomUUID().toString();

		init();
	}

	// Already existing worker - NEED TO UPDATE
	public Worker(String id, String h, int p, WorkerType workerType) {
		hostname = h;
		workerPort = p;

		type = workerType;

		// Set initial status
		status = WorkerStatus.INITIATING;

		this.id =id;

		init();
	}

	private void init() {
		jobQueue = new ConcurrentLinkedQueue<Job>();
		unassignedJobQueue = new ConcurrentLinkedQueue<Job>();
	}

	// ------------------------------------------------------------------------
	// THREAD/RUNNABLE RELATED METHODS
	// ------------------------------------------------------------------------

	public void run() {
		running = true;
		if (getStatus() == WorkerStatus.CREATING) {
			createNewWorker();

			if (getStatus() != WorkerStatus.CREATE_FAIL) {
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
			setStatus(WorkerStatus.INACTIVE);
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
		if (getStatus() != WorkerStatus.CREATING) {
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

		if (jCloudsNova.createWorker(id, type)) {
			// Successfully launched instance - set hostname

			hostname = jCloudsNova.getInstanceHostname();
			workerPort = 8081;

			setStatus(WorkerStatus.CREATED);
		} else {
			if (Master.DEBUG)
				System.out.println(TAG+" could not create new cloud server - status = ");
			setStatus(WorkerStatus.CREATE_FAIL);
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
						setStatus(WorkerStatus.INACTIVE);
						break;
					case TIMEOUT:
						resetLastConnectionTimeoutTime();
						setStatus(WorkerStatus.UNREACHABLE);
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
	public void assignJob(Job job) {
		if (Master.DEBUG)
			System.out.println(getTag()+" Assigning job "+job.getId());

		if (status.equals(WorkerStatus.ACTIVE)) {
			sendAssignJobCommand(job);
		}

		addJobToQueue(job);
	}

	private void sendAssignJobCommand(Job job) {
		workerSocketClient.sendMessage("new_job " + job.getId());
	}

	/**
	 *  Connects to worker - asks to delete job
	 */
	public void deleteJob(Job job) {
		job.setStatus(JobStatus.DELETING_ON_MASTER);
		workerSocketClient.sendMessage("delete_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  Connects to worker - asks to finish job
	 */
	public void stopJob(Job job) {
		job.setStatus(JobStatus.STOPPING_ON_MASTER);
		workerSocketClient.sendMessage("stop_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  When a job finishes on worker - remove from Queue
	 */
	public void jobFinished(Job job) {
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

	public synchronized Calendar estimateQueueCompletionCalendar() {
		Calendar returnCal = Calendar.getInstance();
		returnCal.add(Calendar.MILLISECOND, estimateQueueCompletionTimeInMs().intValue());

		return returnCal;
	}

	public synchronized Long estimateQueueCompletionTimeInMs() {
		Long time = 0L;

		// If worker is creating - or created and waiting for worker to come online
		// Add worker creation time (remaining) to queue completion time
		if (status==WorkerStatus.CREATING || status==WorkerStatus.CREATED) {
			time += Math.max(jCloudsNova.getEstimatedCreationTimeInMs()-jCloudsNova.getElapsedCreationTimeInMs(), 0);
		}

		// Add all job estimated execution time for jobs in queue
		for (Job job : jobQueue) {
			time += job.estimateExecutionTimeInMs(jCloudsNova.getInstanceFlavour());
			if (job.getStatus() == JobStatus.RUNNING) {
				time -= job.getUsedCpuTimeInMs();
			}
		}

		if (Master.DEBUG)
			System.out.println(getTag()+" Estimated time for queue completion is "+time.toString()+" ms");

		return time;
	}

	public synchronized Boolean isAvailable() {
		if (jobQueue.size() > 10) {
			available=false;
		}
		return available;
	}

	public synchronized int getNumJobs() {
		return jobQueue.size();
	}

	// ------------------------------------------------------------------------
	// GETTERS/SETTERS & OTHER SIMPLE PROPERTY METHODS
	// ------------------------------------------------------------------------
	public String getId() {return id;}
	public String getHostname() {return hostname;}

	public synchronized void setStatus(WorkerStatus newStatus) {
		if (Master.DEBUG)
			System.out.printf("%s Updated status: %s%n", getTag(), newStatus.toString());

		switch (newStatus) {
			case ACTIVE:
				// If any jobs are assigned on master server - BUT haven't been sent to Worker (on worker server)
				// Send assign command for each job
				jobQueue.stream().filter(job -> job.getStatus()==JobStatus.ASSIGNED_ON_MASTER).forEach(this::sendAssignJobCommand);
				break;
		}

		if (statusChangeListener != null)
			statusChangeListener.onJobStatusChanged(this, newStatus);

		status = newStatus;
	}
	public synchronized WorkerStatus getStatus() {
		return status;
	}

	public String getHostWithPortString() {
		return hostname +":"+ workerPort;
	}

	public WorkerType getType() {return type;}

	public Boolean isConnected() {return status ==WorkerStatus.ACTIVE;}

	public Boolean isRunning() {return running;}

	public String getTag() {
		return String.format("%s [%s]", TAG, getHostWithPortString());
	}

	public void resetLastConnectionTimeoutTime() {
		connectionTimeoutTime = Calendar.getInstance();
	}
	public Boolean isLastConnectionTimeoutRecent() {
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

	public void setOnStatusChangeListener(StatusChangeListener listener) {
		statusChangeListener = listener;
	}

	public interface StatusChangeListener {
		void onJobStatusChanged(Worker worker, WorkerStatus currentStatus) ;
	}
}