package au.edu.utas.lm_nfs_sg.saas.master;

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
	enum Status {
		INACTIVE, INITIATING, ASSIGNING, PREPARING, QUEUED, STARTING, RUNNING, FINISHING, FINISHED, STOPPING, STOPPED, MIGRATING, ERROR, DELETING, DELETED
	}

	public static Map<String, String> jobClassStringMap = new HashMap<String, String>();

	// ------------------------------------------------------------------------
	// General Job Properties
	// ------------------------------------------------------------------------
	protected String id;
	private Class<? extends Job> jobClass;
	private String jobClassString;
	private String jobImageId;

	private Boolean runOnSharedWorker;
	private MasterWorkerThread worker;

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
	// Status Properties
	// ------------------------------------------------------------------------
	private Status status = Status.INACTIVE;
	private String statusMessage;
	private StatusChangeListener statusChangeListener;

	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	Job(String i) {
		id = i;

		jobClassString = jobClassStringMap.get(this.getClass().toString());
		jobClass = this.getClass();

		jobImageId = "26e87817-068b-4221-85a6-e5658aaa12a3";
		runOnSharedWorker = true;

		createdDate = Calendar.getInstance();

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

	public String getId() {
		return id;
	}

	public String getJobClassString() { return jobClassString; }

	void setWorkerProcess(MasterWorkerThread w) {
		worker = w;
	}

	MasterWorkerThread getWorker() {
		return worker;
	}

	public String getJobImageId () {return jobImageId;}
	void setJobImageId(String imageId) {
		jobImageId = imageId;
	}

	public Boolean getRunOnSharedWorker() {return runOnSharedWorker;}
	void setRunOnSharedWorker(Boolean requiresOwnWorker) {
		runOnSharedWorker = requiresOwnWorker;
	}

	// ------------------------------------------------------------------------
	// Calendar/Time Accessors/Setters
	// ------------------------------------------------------------------------

	Calendar getCreatedDate() {
		return createdDate;
	}

	Calendar getStartDate() {
		return startDate;
	}
	void setStartDate() {
		startDate = Calendar.getInstance();}

	Calendar getFinishDate() {
		return finishDate;
	}
	void setFinishDate() {
		finishDate = Calendar.getInstance();}

	Long getUsedCpuTimeInMs() {
		if (usedCpuTime != null) {
			return usedCpuTime;
		} else {
			if (startDate != null) {
				return Calendar.getInstance().getTimeInMillis() - startDate.getTimeInMillis();
			}
		}
		return 0L;
	}

	void setUsedCpuTimeInMs() {
		if (startDate == null) {
			usedCpuTime = 0L;
		} else {
			usedCpuTime = finishDate.getTimeInMillis() - startDate.getTimeInMillis();
		}
	}

	void resetCpuTimes() {
		startDate = null;
		finishDate = null;
		usedCpuTime = null;
	}

	public Calendar getEstimatedFinishDate() {
		return estimatedFinishDate;
	}

	public void setEstimatedFinishDate(Calendar estimatedFinishDate) {
		this.estimatedFinishDate = estimatedFinishDate;
	}

	public void setEstimatedFinishDateInMsFromNow(Long estimatedFinishMs) {
		estimatedFinishDate = Calendar.getInstance();
		estimatedFinishDate.add(Calendar.MILLISECOND, estimatedFinishMs.intValue());
	}

	// ------------------------------------------------------------------------
	// Calendar/Time Utility Functions
	// ------------------------------------------------------------------------

	static String getCalendarString(Calendar cal) {
		if (cal != null) {
			return DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(cal.getTime());
		}
		return "";
	}

	//https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java
	static String getTimeString(Long ms) {
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

	static Long getCalendarInMsFromNow(Calendar cal) {
		if (cal == null) {
			return 0L;
		}
		return cal.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();
	}


	// ------------------------------------------------------------------------
	// Config/Options Accessors
	// ------------------------------------------------------------------------

	File getJobConfigFile() {
		return jobConfigFile;
	}
	void setJobConfigFile(File file) {
		jobConfigFile = file;
	}

	void setJobConfigJsonString(String config) {
		jobConfigJsonString = config;
	}

	String getJobConfigJsonString() {
		if (jobConfigJsonString != null) {
			return jobConfigJsonString;
		}
		return "";
	}

	public void setLaunchOptions(JsonObject launchOpt) {
		launchOptions = launchOpt;

		String deadlineString = launchOptions.get("deadline").getAsString();
		if (deadlineString != null && !deadlineString.equals("")) {
			deadline = Calendar.getInstance();

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
			try {
				deadline.setTime(sdf.parse(deadlineString));
			} catch (ParseException e) {
				e.printStackTrace();
				deadline = null;
			}
		}

		estimatedRunningTimeForFlavour = new HashMap<>();
	}

	Calendar getDeadline() { return deadline; }

	// ------------------------------------------------------------------------
	// Directories/Files Accessors and Methods
	// ------------------------------------------------------------------------

	File getJobResourcesDirectory() { return jobResourcesDirectory; }
	File getJobConfigDirectory() {
		return jobConfigDirectory;
	}
	File getJobResultsDirectory() {
		return jobResultsDirectory;
	}


	// ProcessJobResourcesDir - need a better name
	// This method is called after a "queue" of job resource files have finished uploading
	// I.e. Through new job form in Web Client

	// It can be used to search through uploaded files to find Job specific configuration files
	// (eg. the xml file for CSIRO Spark) - to then convert the file into JSON which can be used to "autofill" the form

	// This method can be overridden by job subclasses
	JsonObject processJobResourcesDir() {
		return null;
	}

	Boolean deleteJob() {
		deleteDirRecursively(Paths.get(jobDirectory.getAbsolutePath()));
		return true;
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

	Long getEstimatedExecutionTimeForFlavourInMs(Flavor instanceFlavour) {
		if (!estimatedRunningTimeForFlavour.containsKey(instanceFlavour)) {
			Long estimatedExecTime = estimateExecutionTimeInMs(instanceFlavour);
			estimatedRunningTimeForFlavour.put(instanceFlavour, estimatedExecTime);

			return estimatedExecTime;
		} else {
			return estimatedRunningTimeForFlavour.get(instanceFlavour);
		}
	}

	// Should override this with sublcass method
	Long estimateExecutionTimeInMs(Flavor instanceFlavour) {
		Long returnEstimate = (long) (45 * 1000);
		//45 Seconds
		return returnEstimate;
	}

	// ------------------------------------------------------------------------
	// Status Interface, Accessors and Methods
	// ------------------------------------------------------------------------

	Status getStatus() {return status;}

	String getStatusString() {
		if (statusMessage != null) {
			return String.format("%s (%s)", status.toString(), statusMessage);
		} else {
			return status.toString();
		}
	}

	void setStatus(Status newStatus) {
		setStatus(newStatus, null);
	}
	void setStatus(Status newStatus, String newStatusMessage) {
		//System.out.printf("<Job: %s> Status: %s%n", id, newStatus);
		status = newStatus;
		statusMessage = newStatusMessage;

		switch (newStatus) {
			case RUNNING:
				setStartDate();
				break;
			case FINISHED:
			case ERROR:
				getWorker().jobFinished(this);
				setFinishDate();
				setUsedCpuTimeInMs();
				break;
			case STOPPED:
				resetCpuTimes();
		}

		if (statusChangeListener != null)
			statusChangeListener.onStatusChanged(this, newStatus);
	}

	void setOnStatusChangeListener(StatusChangeListener listener) {
		statusChangeListener = listener;
	}

	interface StatusChangeListener {
		void onStatusChanged(Job job, Job.Status currentStatus) ;
	}
}
