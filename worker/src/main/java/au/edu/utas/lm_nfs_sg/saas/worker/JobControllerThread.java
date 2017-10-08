package au.edu.utas.lm_nfs_sg.saas.worker;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Calendar;
import java.util.LinkedList;

public class JobControllerThread implements Runnable {

	//================================================================================
	// Static Properties/Functions
	//================================================================================
	enum Status {
		PREPARING, QUEUED, STARTING, RUNNING, FINISHED, STOPPING, STOPPED, MIGRATING, ERROR, DELETING, DELETED
	}

	public final static String TAG = "<JobController>";

	//================================================================================
	// Properties
	//================================================================================

	private String jobId;
	private Status status;

	private MasterRestClient masterRestClient;

	private File jobDirectory;
	private File jobResourcesDirectory;
	private File jobResultsDirectory;

	private Process jobProcess;
	private Thread jobProcessStdOutThread;
	private Thread jobProcessStdErrThread;
	private volatile Boolean jobProcessInputStreamThreadsRunning;
	private BufferedWriter jobProcessStdIn;

	private LinkedList<LogMessage> logMessageList;

	private volatile Boolean queued;
	private volatile Boolean running = false;
	private volatile Boolean waiting = false;
	private volatile Boolean completed = false;

	//================================================================================
	// Constructors
	//================================================================================

	JobControllerThread(String jId) {
		this(jId, true);
	}

	JobControllerThread(String jId, Boolean inQueue) {
		jobId = jId;
		queued = inQueue;

		masterRestClient=new MasterRestClient(getTag());
		logMessageList = new LinkedList<>();
	}

	//================================================================================
	// Accessors
	//================================================================================

	Boolean isRunning() {return running;}

	Boolean isWaiting() {return waiting;}

	public String getTag() {
		return TAG+"["+jobId+"]";
	}

	void setStatus(Status newStatus) {
		if (status!=newStatus) {
			addNewLogMessage("Job status updated to "+newStatus);
			status = newStatus;
			masterRestClient.updateJobStatus(jobId, newStatus);
		}
	}

	//================================================================================
	// Thread/Runnable Methods
	//================================================================================

	@Override
	public void run() {
		running = true;

		prepareJob();

		while(running) try {
			if (!queued) {
				executeJob();
				stopRunning();
			} else {
				setStatus(Status.QUEUED);
			}

			if (running) {
				System.out.println(getTag() + " waiting");
				waiting = true;
				synchronized (this) {
					while (waiting) {
						wait();
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	synchronized void notifyJobControllerThread() {
		waiting = false;
		notify();
	}

	public void stopRunning() {
		addNewLogMessage("Job is stopping");
		running=false;
		if(isWaiting())
			notifyJobControllerThread();
	}

	//================================================================================
	// Job Methods
	//================================================================================

	private void prepareJob() {
		setStatus(Status.PREPARING);

		addNewLogMessage("Creating job directories");
		jobDirectory = new File("job"+java.io.File.separator + jobId + java.io.File.separator);
		jobDirectory.mkdirs();

		jobResourcesDirectory = new File("job"+java.io.File.separator + jobId + java.io.File.separator +"resources");
		jobResourcesDirectory.mkdirs();

		jobResultsDirectory = new File("job"+java.io.File.separator + jobId + java.io.File.separator +"resources"+  java.io.File.separator +"output");
		jobResultsDirectory.mkdirs();

		addNewLogMessage("Downloading job config files from Master...");
		if (masterRestClient.downloadJobConfigFile(jobId, jobDirectory)) {
			addNewLogMessage("Successfully downloaded job config files from Master");
		} else {
			addNewLogMessage("FAILED to downloaded job config from Master");
		}

		addNewLogMessage("Downloading job resources from Master...");
		if(masterRestClient.downloadJobResources(jobId, jobResourcesDirectory)) {
			addNewLogMessage("Successfully downloaded job resources from Master");
		} else {
			addNewLogMessage("FAILED to downloaded job resources from Master");
		}
	}

	//Adapted from https://stackoverflow.com/questions/25086150/run-a-process-asynchronously-and-read-from-stdout-and-stderr
	private void executeJob() {
		addNewLogMessage("Job starting");
		setStatus(Status.STARTING);

		ProcessBuilder builder = new ProcessBuilder("/opt/csiro.au/spark_batch_fsp/bin/spark-batch", "../job_config.xml");
		builder.directory(jobResourcesDirectory.getAbsoluteFile());

		try {
			jobProcess = builder.start();

			jobProcessStdIn = new BufferedWriter(new OutputStreamWriter(jobProcess.getOutputStream()));

			jobProcessStdOutThread = createProcessStdOutListenerThread(jobProcess.getInputStream(), "StdOut");
			jobProcessStdErrThread = createProcessStdOutListenerThread(jobProcess.getErrorStream(), "StdErr");

			jobProcessInputStreamThreadsRunning = true;
			jobProcessStdOutThread.start();
			jobProcessStdErrThread.start();

			addNewLogMessage("Job is now running");

			setStatus(Status.RUNNING);

			int exitCode = -1;
			try {
				exitCode = jobProcess.waitFor();
				jobProcessInputStreamThreadsRunning = false;
				jobProcessStdOutThread.join();
				jobProcessStdErrThread.join();
			} catch (Exception e) {
				System.out.println(getTag()+" Couldn't join stdout/stderr threads");
				e.printStackTrace();
			}

			addNewLogMessage(String.format("Job process exited - code: %d", exitCode),"JobController");

			// If process executed without error (exitCode-143 = SIGTERM)
			if (exitCode == 0) {
				addNewLogMessage("Uploading job results to Master...");
				if (masterRestClient.uploadJobResultsFolder(jobId, jobResultsDirectory)) {
					addNewLogMessage("Successfully uploaded job results to Master");
					setStatus(Status.FINISHED);
				} else {
					addNewLogMessage("FAILED to upload job results to Master");
					setStatus(Status.ERROR);
				}
			} else if (exitCode == 143) {
				addNewLogMessage("Job process terminated");
			} else {
				addNewLogMessage("Job process failed");
				setStatus(Status.ERROR);
			}

		} catch (IOException e) {
			addNewLogMessage("Job process failed");
			setStatus(Status.ERROR);
			addNewLogMessage("Job exception stack trace: "+e.toString());
		}
	}

	private Thread createProcessStdOutListenerThread(final InputStream is, final String threadTag) {
		return new Thread(() -> {
			addNewLogMessage("Listening to job process "+threadTag);
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
				String line;
				while ((line = reader.readLine()) != null && jobProcessInputStreamThreadsRunning) {
					addNewLogMessage(line, threadTag);
				}
			} catch (Exception e) {
				addNewLogMessage("Job process "+threadTag+" listener FAILED");
				e.printStackTrace();
			}
			addNewLogMessage("Stopped listening to job process "+threadTag);
		});
	}

	private synchronized void addNewLogMessage(String mess) {
		addNewLogMessage(mess, "Main");
	}
	private synchronized void addNewLogMessage(String mess, String source){
		logMessageList.add(new LogMessage(mess, Calendar.getInstance(), source));
		System.out.printf("%s %s: %s%n", getTag(), source, mess);
	}

	synchronized void stopJop(Boolean deleteFiles) {
		if ((status != Status.STOPPED) && (status != Status.FINISHED) && (status != Status.ERROR)) {
			setStatus(Status.STOPPING);
			if (jobProcess != null) {
				jobProcess.destroy();
			}
			if (jobProcessInputStreamThreadsRunning != null) {
				jobProcessInputStreamThreadsRunning = false;
			}
			stopRunning();
			setStatus(Status.STOPPED);
		}

		if (deleteFiles) {

			if (jobDirectory.exists()) {
				setStatus(Status.DELETING);
				addNewLogMessage("Deleting job files");
				deleteDirRecursively(Paths.get(jobDirectory.getAbsolutePath()));
				addNewLogMessage("All job files deleted");
				setStatus(Status.DELETED);
			}
		}
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
}

//================================================================================
// Classes/Interfaces
//================================================================================

class LogMessage {
	String source;
	String message;
	Calendar timestamp;

	LogMessage(String m, Calendar c) {
		source = "JobController";
		message=m;
		timestamp=c;
	}

	LogMessage(String m, Calendar c, String s) {
		source = s;
		message=m;
		timestamp=c;
	}
}
