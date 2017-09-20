package au.edu.utas.lm_nfs_sg.saas.master;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.text.DateFormat;
import java.util.*;


public class Job {
	enum Status {
		INACTIVE, ACTIVE, INITIATING, MIGRATING, ERROR, UNREACHABLE, FINISHED
	}

	public final static Map<Class<? extends Job>, String> jobTypes = new HashMap<Class<? extends Job>, String>();

	static {
		jobTypes.put(SparkJob.class, "CSIRO Spark");
		jobTypes.put(FreqCountJob.class, "Freq Count Stream");
	}

	protected String id;
	private Class<? extends Job> jobClass;
	private String jobType;
	private String jobImageId;
	private Boolean jobRequiresOwnWorker;

	private String jobDescription;

	String jobParamsJsonString;
	ArrayList<String> jobResourcesUrls;
	private File jobParamsDirectory;
	private File jobResourcesDirectory;

	private Calendar startTime;
	private Calendar endTime;
	private Long cpuTimeMs;

	private Status status = Status.INACTIVE;
	private StatusChangeListener statusChangeListener;

	private MasterWorkerThread worker;

	Job(String i) {
		id = i;

		jobType = this.getClass().toString();
		jobClass = this.getClass();

		jobImageId = "26e87817-068b-4221-85a6-e5658aaa12a3";
		jobRequiresOwnWorker = false;

		jobResourcesDirectory = new File("job/"+id+"/res");
		jobResourcesDirectory.mkdirs();

		jobParamsDirectory = new File("job/"+id+"/params");
		jobParamsDirectory.mkdirs();
	}

	public String getJobImageId () {return jobImageId;}
	protected void setJobImageId(String imageId) {
		jobImageId = imageId;
	}

	public Boolean getJobRequiresOwnWorker () {return jobRequiresOwnWorker;}
	protected void setJobRequiresOwnWorker(Boolean requiresOwnWorker) {
		jobRequiresOwnWorker = requiresOwnWorker;
	}

	void setJobParamsJsonString(String params) {
		jobParamsJsonString = params;
	}

	protected String getJobParamsJsonString () {
		if (jobParamsJsonString != null) {
			return jobParamsJsonString;
		}
		return "";
	}

	JSONObject processJobResourcesDir() {
		return null;
	}

	protected JSONArray getJobResourcesDirFilenames() {
		JSONArray uploadedFilenames = new JSONArray();

		for (File file : getJobResourcesDirectory().listFiles()) {
			if (file.isFile()) {
				uploadedFilenames.add("./" + file.getName());
			}
		}

		return uploadedFilenames;
	}

	public File getJobResourcesDirectory() {
		return jobResourcesDirectory;
	}

	public File getJobParamsDirectory() {
		return jobParamsDirectory;
	}

	public String getId() {
		return id;
	}

	public String getJobType () { return jobType; }

	Status getStatus() {return status;}
	String getStatusString() {
		return "job "+getId()+" is "+getStatus().toString();
	}
	void setStatus(Status newStatus) {
		status = newStatus;
		if (statusChangeListener != null)
			statusChangeListener.onStatusChanged(this, newStatus);
	}
	void setOnStatusChangeListener(StatusChangeListener listener) {
		statusChangeListener = listener;
	}

	void setWorkerProcess(MasterWorkerThread w) {
		worker = w;
	}

	MasterWorkerThread getWorker() {
		return worker;
	}

	void setStartCpuTime() {startTime = Calendar.getInstance();}
	void setEndCpuTime() {endTime = Calendar.getInstance();}
	Long getCpuTimeMs() {
		if (cpuTimeMs == null) {
			return 0L;
		}
		return cpuTimeMs;
	}

	String getStartTimeString() {
		if (startTime!=null) {
			DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(startTime.getTime());
		}
		return "";
	}

	void finishJob() {
		setEndCpuTime();
		cpuTimeMs = endTime.getTimeInMillis() - startTime.getTimeInMillis();
		setStatus(Status.FINISHED);
	}

	interface StatusChangeListener {
		void onStatusChanged(Job job, Job.Status currentStatus) ;
	}
}
