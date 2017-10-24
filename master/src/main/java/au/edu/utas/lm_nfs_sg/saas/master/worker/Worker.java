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
import java.util.Deque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class Worker implements Runnable {

	private static final String  TAG = "<Worker>";
	private static final int SOCKET_DEFAULT_CONN_RETRY_DELAY_MS = 50;
	private static final int SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS = 10; // 500ms max (10 attempts * 50ms apart)
	private static final int SOCKET_CREATEWORKER_CONN_RETRY_DELAY_MS = 500;
	private static final int SOCKET_CREATEWORKER_CONN_RETRY_ATTEMPTS = 100; // 5 minutes max (100 attempts * 500ms apart)

	private JCloudsNova jCloudsNova;
	private Flavor instanceFlavour;

	private String workerId;
	private String hostname;
	private int workerPort;

	private Thread workerSocketThread;
	private SocketClient workerSocketClient;

	private WorkerType type = WorkerType.PUBLIC;
	private Deque<Job> jobQueue;
	private Deque<Job> unassignedJobQueue;

	private Boolean available = true;

	private Calendar connectionTimeoutTime;

	private WorkerStatus status;
	private StatusChangeListener statusChangeListener;

	private volatile Boolean running=false;

	private final Boolean jobQueueSynchronise = true;
	private final Boolean statusSynchronise = true;

	// ------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------

	// Create new worker/cloud instance
	public Worker(Flavor instanceFlav, WorkerType workerType) {
		hostname = "unknown.hostname";
		workerPort = 0;

		this.instanceFlavour = instanceFlav;

		type = workerType;

		status = WorkerStatus.NOT_CREATED;

		workerId = UUID.randomUUID().toString();

		jCloudsNova = new JCloudsNova(instanceFlavour, workerId, type);

		init();
	}

	// Already existing worker - NEED TO UPDATE
	public Worker(String id, String h, int p, WorkerType workerType, String instanceId, Flavor instanceFlav) {
		hostname = h;
		workerPort = p;
		instanceFlavour = instanceFlav;

		type = workerType;

		// Set initial status
		status = WorkerStatus.INACTIVE;

		workerId =id;

		jCloudsNova = new JCloudsNova(instanceId, instanceFlavour, workerId, type);

		init();
	}

	private void init() {
		jobQueue = new ConcurrentLinkedDeque<>();
		unassignedJobQueue = new ConcurrentLinkedDeque<>();
	}

	// ------------------------------------------------------------------------
	// THREAD/RUNNABLE RELATED METHODS
	// ------------------------------------------------------------------------

	public void run() {
		running = true;
		if (getStatus() == WorkerStatus.NOT_CREATED) {

			createNewWorker();

			if (getStatus() == WorkerStatus.CREATED) {
				// Create socket and attempt to connect to worker
				startWorkerSocketThread();
			}

		} else {
			startWorkerSocketThread();
		}

		if (workerSocketThread != null) {
			try {
				workerSocketThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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

			/*while (!isConnected()) {
				try {
					// If connecting timeout occurred within the last 10 minutes - break from loop
					if (isLastConnectionTimeoutRecent())
						break;

					// Sleep for socket to exhaust all connection attempts (with delay)
					Thread.sleep(SOCKET_DEFAULT_CONN_RETRY_DELAY_MS * SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}*/
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
		setStatus(WorkerStatus.CREATING);
		if (Master.DEBUG)
			System.out.println(TAG+ " creating new worker");

		if (jCloudsNova.createWorker()) {
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
	private void startWorkerSocketThread() {
		if (workerSocketThread == null) {
			workerSocketClient = new SocketClient(getTag() + " [WorkerClient]", hostname, workerPort);

			if (getStatus()==WorkerStatus.CREATED) {
				workerSocketClient.setConnectionRetryDelayMs(SOCKET_CREATEWORKER_CONN_RETRY_DELAY_MS);
				workerSocketClient.setConnectionRetries(SOCKET_CREATEWORKER_CONN_RETRY_ATTEMPTS); // 0 - indicates unlimited retries
			} else {
				workerSocketClient.setConnectionRetryDelayMs(SOCKET_DEFAULT_CONN_RETRY_DELAY_MS);
				workerSocketClient.setConnectionRetries(SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS);
			}

			workerSocketClient.setOnMessageReceivedListener((SocketClient.MessageCallback) this::receivedMessageFromWorker);

			workerSocketClient.setOnStatusChangeListener((socketCommunication, currentStatus) -> {
				switch (currentStatus) {
					case CONNECTED:
						// If connected to a newly created worker - reset connection retry parameters to default
						if (getStatus()==WorkerStatus.CREATED) {
							jCloudsNova.launchSuccessful();

							workerSocketClient.setConnectionRetryDelayMs(SOCKET_DEFAULT_CONN_RETRY_DELAY_MS);
							workerSocketClient.setConnectionRetries(SOCKET_DEFAULT_CONN_RETRY_ATTEMPTS);
						}
						break;
					case DISCONNECTED:
						setStatus(WorkerStatus.INACTIVE);
						break;
					case TIMEOUT:
						if (getStatus()==WorkerStatus.CREATED) {
							setStatus(WorkerStatus.CREATE_FAIL);
							jCloudsNova.launchFailed();
							stopRunning();
						} else {
							resetLastConnectionTimeoutTime();
							setStatus(WorkerStatus.UNREACHABLE);
						}
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
		workerSocketClient.sendMessage("delete_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  Connects to worker - asks to finish job
	 */
	public void stopJob(Job job) {
		workerSocketClient.sendMessage("stop_job "+job.getId());

		removeJobFromQueue(job);
	}

	/**
	 *  When a job finishes on worker - remove from Queue
	 */
	public void jobFinished(Job job) {
		removeJobFromQueue(job);
		if (getNumJobs() <= 1) {
			Master.workerIsFree(type);
		}
	}


	// ------------------------------------------------------------------------
	// JOB QUEUE METHODS
	// ------------------------------------------------------------------------
	private void addJobToQueue(Job job) {
		synchronized (jobQueueSynchronise) {
			jobQueue.add(job);
		}
	}

	private void removeJobFromQueue(Job job) {
		synchronized (jobQueueSynchronise) {
			if (jobQueue.contains(job))
				jobQueue.remove(job);
		}
	}

	public void rejectMostRecentUncompletedJob() {
		synchronized (jobQueueSynchronise) {
			if (getNumJobs() > 1) {
				Job rejectJob = jobQueue.peekLast();
				if (rejectJob.getStatus() != JobStatus.RUNNING) {
					System.out.printf("%s rejecting job: %s%n", getTag(), rejectJob.getId());
					stopJob(rejectJob);
					rejectJob.rejectedFromWorker();
				}
			}
		}
	}

	public void rejectAllUncompletedJobs() {
		synchronized (jobQueueSynchronise) {
			jobQueue.forEach(Job::rejectedFromWorker);
			jobQueue.clear();
		}
	}

	public Calendar estimateQueueCompletionCalendar() {
		Calendar returnCal = Calendar.getInstance();
		returnCal.add(Calendar.MILLISECOND, estimateQueueCompletionTimeInMs().intValue());

		return returnCal;
	}

	public Long estimateQueueCompletionTimeInMs() {
		synchronized (jobQueueSynchronise) {
			Long time = 0L;

			// If worker is creating - or created and waiting for worker to come online
			// Add worker creation time (remaining) to queue completion time
			if (status == WorkerStatus.CREATING || status == WorkerStatus.CREATED) {
				time += Math.max(jCloudsNova.getEstimatedCreationTimeInMs() - jCloudsNova.getElapsedCreationTimeInMs(), 0);
			}

			// Add all job estimated execution time for jobs in queue
			for (Job job : jobQueue) {
				time += job.getEstimatedExecutionTimeForFlavourInMs(jCloudsNova.getInstanceFlavour());
				if (job.getStatus() == JobStatus.RUNNING) {
					time -= job.getUsedCpuTimeInMs();
				}
			}

			if (Master.DEBUG)
				System.out.println(getTag() + " Estimated time for queue completion is " + time.toString() + " ms");

			return time;
		}
	}

	public Boolean isAvailable() {
		synchronized (jobQueueSynchronise) {
			if (jobQueue.size() > 10) {
				available = false;
			}
			return available;
		}
	}

	public int getNumJobs() {
		synchronized (jobQueueSynchronise) {
			return jobQueue.size();
		}
	}

	// ------------------------------------------------------------------------
	// GETTERS/SETTERS & OTHER SIMPLE PROPERTY METHODS
	// ------------------------------------------------------------------------
	public String getWorkerId() {return workerId;}
	public String getHostname() {return hostname;}

	void setStatus(WorkerStatus newStatus) {
		synchronized (statusSynchronise) {
			if (Master.DEBUG)
				System.out.printf("%s Updated status: %s%n", getTag(), newStatus.toString());

			switch (newStatus) {
				case ACTIVE:
					// If any jobs are assigned on master server - BUT haven't been sent to Worker (on worker server)
					// Send assign command for each job
					jobQueue.stream().filter(job -> job.getStatus() == JobStatus.ASSIGNED_ON_MASTER).forEach(this::sendAssignJobCommand);
					break;
			}

			if (statusChangeListener != null) {
				try {
					statusChangeListener.onJobStatusChanged(this, newStatus);
				} catch (RuntimeException e) {
					e.printStackTrace();
				}
			}

			status = newStatus;
		}
	}
	public WorkerStatus getStatus() {
		synchronized (statusSynchronise) {
			return status;
		}
	}

	public Boolean updateStatusFromWorkerNode(String workerStatus) {
		try {
			setStatus(WorkerStatus.valueOf(workerStatus));
			return true;
		} catch (IllegalArgumentException e) {
			System.out.println(TAG+" Illegal Argument Exception - Setting worker status to "+workerStatus+" - workerId="+ workerId);
			return false;
		}
	}

	public String getHostWithPortString() {
		return hostname +":"+ workerPort;
	}

	public WorkerType getType() {return type;}

	public Boolean isConnected() {return getStatus() ==WorkerStatus.ACTIVE;}

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
		synchronized (statusSynchronise) {
			statusChangeListener = listener;
		}
	}

	public interface StatusChangeListener {
		void onJobStatusChanged(Worker worker, WorkerStatus currentStatus) ;
	}
}