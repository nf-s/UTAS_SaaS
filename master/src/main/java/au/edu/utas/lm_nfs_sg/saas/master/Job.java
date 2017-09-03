package au.edu.utas.lm_nfs_sg.saas.master;

import java.text.DateFormat;
import java.util.Calendar;


class Job {
	enum Status {
		INACTIVE, ACTIVE, INITIATING, MIGRATING, ERROR, UNREACHABLE, FINISHED
	}

	private String id;
	private Status status = Status.INACTIVE;

	private String jobParametersJSON;

	private String streamHostname;
	private int streamPort;
	private int kFreqWords;

	private MasterWorkerThread worker;
	private int workerProcessPort;

	private Calendar startTime;
	private Calendar endTime;
	private Long cpuTimeMs;

	private String results;
	private Boolean currentlyRetrievingResults = false;

	private StatusChangeListener statusChangeListener;

	Job(String i, String sh, int sp, int k) {
		id = i;
		streamHostname = sh;
		streamPort = sp;
		kFreqWords = k;
	}

	String getId() {
		return id;
	}

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

	int getkFreqWords() {
		return kFreqWords;
	}

	String getStreamHostname() {
		return streamHostname;
	}

	int getStreamPort() {
		return streamPort;
	}

	void setWorkerProcess(MasterWorkerThread w) {
		worker = w;
	}

	MasterWorkerThread getWorker() {
		return worker;
	}

	int getWorkerProcessPort() {
		return workerProcessPort;
	}
	void setWorkerProcessPort(int p) {
		workerProcessPort = p;
	}

	void setStartCpuTime() {startTime = Calendar.getInstance();}
	void setEndCpuTime() {endTime = Calendar.getInstance();}
	Long getCpuTimeMs() {
		if (cpuTimeMs == null) {
			return 0L;
		}
		return cpuTimeMs;
	}

	String getBill() {
		return "Start time: "+DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(startTime.getTime())
				+"\nEnd time: "+DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(endTime.getTime())
				+"\nTime used: "+getCpuTimeMs()+" Milliseconds"
				+"\n\nAmount Due: $"+String.format("%.2f", (cpuTimeMs/1000)*.01)+" ($0.01 per second)";
	}

	void resetResults() {results = null;}
	void setResults(String r) {results = r;}
	void addResults(String r) {results += "\n"+r;}
	String getResults(){
		return results;
	}
	String getResultsString() {
		if (results == null) {
			return "No results available.";
		} else if (!currentlyRetrievingResults) {
			return results;
		} else {
			return results+"\nRESULTS STILL BEING RECEIVED \n\n Refresh the page for more results";
		}
	}
	void setCurrentlyRetrievingResults(Boolean r) {
		currentlyRetrievingResults = r;}
	Boolean isCurrentlyRetrievingResults() {return currentlyRetrievingResults;}

	void finishJob() {
		setEndCpuTime();
		cpuTimeMs = endTime.getTimeInMillis() - startTime.getTimeInMillis();
		setStatus(Status.FINISHED);
	}

	interface StatusChangeListener {
		void onStatusChanged(Job job, Job.Status currentStatus) ;
	}
}
