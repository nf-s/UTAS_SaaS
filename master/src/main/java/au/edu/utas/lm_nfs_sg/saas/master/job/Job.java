package au.edu.utas.lm_nfs_sg.saas.master.job;

import au.edu.lm_nf_sg.saas.common.job.JobStatus;
import au.edu.lm_nf_sg.saas.common.job.JobType;
import au.edu.lm_nf_sg.saas.common.worker.WorkerType;
import au.edu.utas.lm_nfs_sg.saas.master.worker.JCloudsNova;
import au.edu.utas.lm_nfs_sg.saas.master.Master;
import au.edu.utas.lm_nfs_sg.saas.master.worker.Worker;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Job {
	// ------------------------------------------------------------------------
	// Static enums, variables and methods
	// ------------------------------------------------------------------------
	private static final String  TAG = "<Job>";

	public static Map<String, String> jobClassStringMap = new HashMap<String, String>();

	// ------------------------------------------------------------------------
	// General Job Properties
	// ------------------------------------------------------------------------
	protected String id;
	private Class<? extends Job> jobClass;
	private String jobClassString;

	private JobType jobType;
	private WorkerType workerType;
	private Worker worker;
	private String jobImageId;

	private String jobDescription;

	private Calendar createdDate;
	private Calendar startDate;
	private Calendar finishDate;
	private Long usedCpuTime;

	// ------------------------------------------------------------------------
	// Config and Directories/Files Properties
	// ------------------------------------------------------------------------
	private File jobConfigFile;
	private String jobConfigJsonString;
	private File jobDirectory;
	private File jobConfigDirectory;
	private File jobResourcesDirectory;
	private File jobResultsDirectory;

	private JsonObject launchOptions;
	private Calendar deadline;

	private Map<Flavor, Long> estimatedRunningTimeForFlavour;
	private Calendar estimatedFinishDate;

	// ------------------------------------------------------------------------
	// WorkerStatus Properties
	// ------------------------------------------------------------------------
	private JobStatus status = JobStatus.INACTIVE;
	private String statusMessage;
	private StatusChangeListener statusChangeListener;

	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	public Job(String i) {
		id = i;

		jobClassString = jobClassStringMap.get(this.getClass().toString());
		jobClass = this.getClass();

		createdDate = Calendar.getInstance();

		// Set default values:
		if (jobImageId==null)
			jobImageId = JCloudsNova.DEFAULT_IMAGE_ID;
		if (jobType==null)
			jobType = JobType.BOUNDED;
		if (workerType==null)
			workerType = WorkerType.PUBLIC;

		// Create required directories
		jobDirectory = new File("job"+java.io.File.separator +id);
		jobDirectory.mkdirs();

		jobResourcesDirectory = new File("job"+java.io.File.separator +id+java.io.File.separator +"resources");
		jobResourcesDirectory.mkdirs();
		
		jobResultsDirectory = new File("job"+java.io.File.separator +id+java.io.File.separator +"results");
		jobResultsDirectory.mkdirs();

		jobConfigDirectory = new File("job"+java.io.File.separator+id+java.io.File.separator +"config");
		jobConfigDirectory.mkdirs();
	}

	// ------------------------------------------------------------------------
	// General Accessors/Setters
	// ------------------------------------------------------------------------

	public String getTag() {
		return TAG+" "+ getId();
	}

	public String getId() {
		return id;
	}

	public String getJobClassString() { return jobClassString; }

	void setWorker(Worker w) {
		worker = w;
	}

	public Worker getWorker() {
		return worker;
	}

	public String getJobImageId () {return jobImageId;}
	void setJobImageId(String imageId) {
		jobImageId = imageId;
	}

	public WorkerType getWorkerType() {return workerType;}
	void setWorkerType(WorkerType type) {
		workerType = type;
	}

	public JobType getJobType() {return jobType;}
	void setJobType(JobType type) {
		jobType = type;
	}

	// ------------------------------------------------------------------------
	// Calendar/Time Accessors/Setters
	// ------------------------------------------------------------------------

	public Calendar getCreatedDate() {
		return createdDate;
	}

	public Calendar getStartDate() {
		return startDate;
	}
	private void setStartDate() {
		startDate = Calendar.getInstance();}

	public Calendar getFinishDate() {
		return finishDate;
	}
	private void setFinishDate() {
		finishDate = Calendar.getInstance();}

	public Long getUsedCpuTimeInMs() {
		if (usedCpuTime != null) {
			return usedCpuTime;
		} else {
			if (startDate != null) {
				return Calendar.getInstance().getTimeInMillis() - startDate.getTimeInMillis();
			}
		}
		return 0L;
	}

	private void setUsedCpuTimeInMs() {
		if (startDate == null) {
			usedCpuTime = 0L;
		} else {
			usedCpuTime = finishDate.getTimeInMillis() - startDate.getTimeInMillis();
		}
	}

	private void resetCpuTimes() {
		startDate = null;
		finishDate = null;
		usedCpuTime = null;
	}

	public Calendar getEstimatedFinishDate() {
		return estimatedFinishDate;
	}

	private void setEstimatedFinishDate() {
		estimatedFinishDate = Calendar.getInstance();
		estimatedFinishDate.add(Calendar.MILLISECOND, (int)(worker.estimateQueueCompletionTimeInMs()
				+getEstimatedExecutionTimeForFlavourInMs(worker.getInstanceFlavour())));
	}

	public Long getEstimatedExecutionTimeForFlavourInMs(Flavor instanceFlavour) {
		if (!estimatedRunningTimeForFlavour.containsKey(instanceFlavour)) {
			Long estimatedExecTime = estimateExecutionTimeInMs(instanceFlavour);
			estimatedRunningTimeForFlavour.put(instanceFlavour, estimatedExecTime);

			return estimatedExecTime;
		} else {
			return estimatedRunningTimeForFlavour.get(instanceFlavour);
		}
	}

	// Should override this with sublcass method
	public Long estimateExecutionTimeInMs(Flavor instanceFlavour) {
		Long returnEstimate = (long) (45 * 1000);
		//45 Seconds
		return returnEstimate;
	}

	public Calendar getDeadline() { return deadline; }
	private void setDeadline(Calendar d) {  deadline=d; }


	// ------------------------------------------------------------------------
	// Calendar/Time Utility Functions
	// ------------------------------------------------------------------------

	public static String getCalendarString(Calendar cal) {
		if (cal != null) {
			return DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(cal.getTime());
		}
		return "";
	}

	//https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java
	public static String getTimeString(Long ms) {
		if (ms != 0) {
			if (TimeUnit.MILLISECONDS.toMinutes(ms) == 0) {
				return String.format("%02d sec",
						TimeUnit.MILLISECONDS.toSeconds(ms)
				);
			} else {
				return String.format("%02d min, %02d sec",
						TimeUnit.MILLISECONDS.toMinutes(ms),
						TimeUnit.MILLISECONDS.toSeconds(ms) -
								TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(ms))
				);
			}
		}
		return "";
	}

	public static Long getCalendarInMsFromNow(Calendar cal) {
		if (cal == null) {
			return 0L;
		}
		return cal.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();
	}

	// ------------------------------------------------------------------------
	// Config/Options Accessors
	// ------------------------------------------------------------------------

	public JsonElement getSerializedJsonElement() {
		GsonBuilder gsonB = new GsonBuilder();
		gsonB.registerTypeAdapter(Job.class, new JobJSONSerializer());
		Gson gson = gsonB.create();
		return gson.toJsonTree(this, Job.class);
	}

	public File getJobConfigFile() {
		return jobConfigFile;
	}
	void setJobConfigFile(File file) {
		jobConfigFile = file;
	}

	public void setJobConfigJsonString(String config) {
		jobConfigJsonString = config;
	}

	public String getJobConfigJsonString() {
		if (jobConfigJsonString != null) {
			return jobConfigJsonString;
		}
		return "";
	}

	public void setLaunchOptionsFromJson(JsonObject launchOpt) {
		launchOptions = launchOpt;

		String deadlineString = launchOptions.get("deadline").getAsString();
		if (deadlineString != null && !deadlineString.equals("")) {
			Calendar deadline = Calendar.getInstance();

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
			try {
				deadline.setTime(sdf.parse(deadlineString));
			} catch (ParseException e) {
				e.printStackTrace();
				deadline = null;
			}

			setDeadline(deadline);
		}

		estimatedRunningTimeForFlavour = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	// Directories/Files Accessors and Methods
	// ------------------------------------------------------------------------

	public File getJobResourcesDirectory() { return jobResourcesDirectory; }
	public File getJobConfigDirectory() {
		return jobConfigDirectory;
	}
	public File getJobResultsDirectory() {
		return jobResultsDirectory;
	}


	// ProcessJobResourcesDir - need a better name
	// This method is called after a "queue" of job resource files have finished uploading
	// I.e. Through new job form in Web Client

	// It can be used to search through uploaded files to find Job specific configuration files
	// (eg. the xml file for CSIRO Spark) - to then convert the file into JSON which can be used to "autofill" the form

	// This method can be overridden by job subclasses
	public JsonObject processJobResourcesDir() {
		return null;
	}

	private void deleteDirRecursively(Path directory) {
		try {
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// ------------------------------------------------------------------------
	// Job Methods
	// ------------------------------------------------------------------------

	public void activateWithJson(JsonObject launchOptions) {
		setStatus(JobStatus.INITIATING);
		setLaunchOptionsFromJson(launchOptions);
	}

	public void preparingForAssigning() {
		setStatus(JobStatus.ASSIGNING);
	}

	public void assignToWorker(Worker w) {
		setStatus(JobStatus.ASSIGNED_ON_MASTER);

		setWorker(w);

		if (jobType==JobType.UNBOUNDED)
			setEstimatedFinishDate();
	}

	// Called from setStatus() - triggered by worker node setting status
	private void onRunning() {
		setStartDate();
	}

	public void stop() {
		setStatus(JobStatus.STOPPING_ON_MASTER);
	}

	// Called from setStatus() - triggered by worker node setting status
	private void onStopped() {
		resetCpuTimes();
	}

	public Boolean delete() {
		setStatus(JobStatus.DELETING_ON_MASTER);

		deleteDirRecursively(Paths.get(jobDirectory.getAbsolutePath()));
		return true;
	}

	// Called from setStatus() - triggered by worker node setting status
	private void onFailure() {
		getWorker().jobFinished(this);
		setFinishDate();
		setUsedCpuTimeInMs();
		if (Master.DEBUG)
			System.out.printf("%s Execution time: %dms%n", getTag(), getUsedCpuTimeInMs());
	}

	// ------------------------------------------------------------------------
	// WorkerStatus Interface, Accessors and Methods
	// ------------------------------------------------------------------------

	public JobStatus getStatus() {return status;}

	public String getStatusString() {
		if (statusMessage != null && !statusMessage.equals("")) {
			return String.format("%s: %s", status.toString(), statusMessage);
		} else {
			return status.toString();
		}
	}

	private void setStatus(JobStatus newStatus) {
		setStatus(newStatus, "");
	}
	private void setStatus(JobStatus newStatus, String newStatusMessage) {
		if (Master.DEBUG)
			System.out.printf("%s Updated status: %s %s%n" , getTag(), newStatus.toString(), newStatusMessage);

		switch (newStatus) {
			case RUNNING:
				onRunning();
				break;
			case FINISHED:
			case ERROR:
				onFailure();
				break;
			case STOPPED:
				onStopped();
		}

		if (statusChangeListener != null)
			statusChangeListener.onStatusChanged(this, newStatus);

		status = newStatus;
		statusMessage = newStatusMessage;
	}

	public Boolean updateStatusFromWorkerNode(String jobStatus) {
		try {
			setStatus(JobStatus.valueOf(jobStatus));
			return true;
		} catch (IllegalArgumentException e) {
			System.out.println(TAG+" Illegal Argument Exception - Setting job status to "+jobStatus+" - jobId="+id);
			return false;
		}
	}

	public void setOnStatusChangeListener(StatusChangeListener listener) {
		statusChangeListener = listener;
	}

	public interface StatusChangeListener {
		void onStatusChanged(Job job, JobStatus currentStatus) ;
	}
}