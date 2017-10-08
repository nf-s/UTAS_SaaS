package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.gson.JsonObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Job {
	// ------------------------------------------------------------------------
	// Static enums, variables and methods
	// ------------------------------------------------------------------------
	enum Status {
		INACTIVE, ASSIGNING, PREPARING, QUEUED, STARTING, RUNNING, FINISHED, STOPPING, STOPPED, MIGRATING, ERROR, DELETING, DELETED
	}

	public static Map<String, String> jobClassStringMap = new HashMap<String, String>();

	// ------------------------------------------------------------------------
	// General Properties
	// ------------------------------------------------------------------------
	protected String id;
	private Class<? extends Job> jobClass;
	private String jobClassString;
	private String jobImageId;

	private Boolean canRunOnSharedWorker;
	private MasterWorkerThread worker;

	private String jobDescription;

	private Calendar createdDate;
	private Calendar startDate;
	private Calendar endDate;
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

	// ------------------------------------------------------------------------
	// Status Properties
	// ------------------------------------------------------------------------
	private Status status = Status.INACTIVE;
	private StatusChangeListener statusChangeListener;

	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	Job(String i) {
		id = i;

		jobClassString = jobClassStringMap.get(this.getClass().toString());
		jobClass = this.getClass();

		jobImageId = "26e87817-068b-4221-85a6-e5658aaa12a3";
		canRunOnSharedWorker = true;

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
	// General Accessors
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

	public Boolean getCanRunOnSharedWorker() {return canRunOnSharedWorker;}
	void setCanRunOnSharedWorker(Boolean requiresOwnWorker) {
		canRunOnSharedWorker = requiresOwnWorker;
	}

	Calendar getCreatedDate() {
		return createdDate;
	}

	void setStartCpuTime() {
		startDate = Calendar.getInstance();}
	void setEndCpuTime() {
		endDate = Calendar.getInstance();}

	Long getUsedCpuTime() {
		if (usedCpuTime != null) {
			return usedCpuTime;
		} else {
			if (startDate != null) {
				return Calendar.getInstance().getTimeInMillis() - startDate.getTimeInMillis();
			}
		}
		return 0L;
	}

	void setUsedCpuTime() {
		if (startDate == null) {
			usedCpuTime = 0L;
		} else {
			usedCpuTime = endDate.getTimeInMillis() - startDate.getTimeInMillis();
		}
	}

	void resetCpuTimes() {
		startDate = null;
		endDate = null;
		usedCpuTime = null;
	}

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

	// ------------------------------------------------------------------------
	// Config Accessors
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

	// ------------------------------------------------------------------------
	// Directories/Files Accessors
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
	// Status Interface, Accessors and Methods
	// ------------------------------------------------------------------------

	Status getStatus() {return status;}
	void setStatus(Status newStatus) {
		//System.out.printf("<Job: %s> Status: %s%n", id, newStatus);
		status = newStatus;

		switch (newStatus) {
			case RUNNING:
				setStartCpuTime();
				break;
			case FINISHED:
			case ERROR:
				setEndCpuTime();
				setUsedCpuTime();
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
