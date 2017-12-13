package au.edu.utas.lm_nfs_sg.saas.master;

import au.edu.utas.lm_nfs_sg.saas.master.job.Job;
import au.edu.utas.lm_nfs_sg.saas.master.worker.JCloudsNova;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;

public class PerformanceEvaluation implements Runnable {
	public final String TAG = "<PERFORMANCE EVALUATION> ";
	private static HashMap<String, String> jobTemplates = new HashMap<>();

	private static LinkedList<PerformanceEvaluation> queue = new LinkedList<>();
	private static LinkedList<PerformanceEvaluation> finished = new LinkedList<>();
	private static PerformanceEvaluation currentPerformanceEvaluation;

	static {
		jobTemplates.put("s", "small-test(94)");
		jobTemplates.put("m", "medium-test(Forcett)");
		jobTemplates.put("l", "large-test(Wangary)");
	}

	JCloudsNova jCloudsNova;

	private int delayBeforeStartInMs = 3000; // 3 seconds
	private int delayBetweenJobSubmissionInMs = 100;
	private int delayBetweenTrialsInMs = 3000; // 10 seconds

	private int currentSample;
	private int sampleSize;

	private int trialNumJobs;
	private int trialDeadlineModifier;
	private String[] trialJobTemplates;

	private ArrayList<ArrayList<String>> workerResults = new ArrayList<>();

	private final class Lock { }
	private final Object lock = new Lock();
	private volatile boolean running = true;
	private volatile boolean waiting = false;

	PerformanceEvaluation(JCloudsNova jCloudsNova, int sampleSize, int trialDeadlineModifier, int trialNumJobs, String[] trialJobTemplates) {
		this.jCloudsNova = jCloudsNova;

		this.sampleSize = sampleSize;

		this.trialDeadlineModifier = trialDeadlineModifier;
		this.trialJobTemplates = trialJobTemplates;
		this.trialNumJobs = trialNumJobs;
	}

	public void addToQueue(PerformanceEvaluation performanceEvaluation) {
		queue.add(performanceEvaluation);
	}

	public static PerformanceEvaluation getCurrentPerformanceEvaluation() {return currentPerformanceEvaluation;}

	/**
	 * Performance Evaluation function. Can use three different job templates with varying deadlines
	 * and can submit multiple jobs at a constant rate.
	 *
	 * Possible job templates are:
	 * 		small-test(94)
	 * 		medium-test(Forcett)
	 * 		large-test(Wangary)
	 *
	 * 	The results of each trial are printed when a job successfully completes. Worker resource utilisation is printed
	 * 	after all jobs complete {@see #allWorkersAreFree()}
	 */

	@Override
	public void run() {
		currentPerformanceEvaluation = this;

		currentSample = 1;

		running = true;

		while (running) {
			if (currentSample == 1) {
				try {
					Thread.sleep(delayBeforeStartInMs);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {

				try {
					Thread.sleep(delayBetweenTrialsInMs);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

			System.out.println("\n\n" + TAG + "Starting Sample "+currentSample);

			for (int j = 0; j < trialNumJobs; j++) {
				System.out.println(TAG + "Creating Job "+(j+1)+"/"+trialNumJobs+" (Sample "+currentSample+")");

				int finalJ = j;
				new Thread(() -> {
					Job newJob = Master.createJob();

					newJob.loadTemplate(jobTemplates.get(trialJobTemplates[finalJ % trialJobTemplates.length]));

					Calendar deadline = Calendar.getInstance();

					int averageExecTime = newJob.getEstimatedExecutionTimeForFlavourInMs(JCloudsNova.getDefaultFlavour()).intValue();

					deadline.add(Calendar.MILLISECOND, averageExecTime * trialDeadlineModifier);

					Master.activateJob(newJob.getId(), (JsonObject) new JsonParser()
							.parse("{\"deadline\":\"" +
									Job.deadlineDateTimeStringFormat.format(deadline.getTime()) + "\"}"));
				}).start();

				// Delay between job submission
				try {
					Thread.sleep(delayBetweenJobSubmissionInMs);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			try {
				System.out.println(TAG + "Waiting for Sample "+currentSample);
				// Wait for jobs to finish
				waiting = true;

				while (waiting) {
					synchronized (lock) {
						lock.wait();
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println(TAG + "Finished Sample "+currentSample);

			jCloudsNova.terminateAll();
			boolean waitingForTermination = true;
			int waitingForTerminationCounter = 0;

			while (waitingForTermination) {
				System.out.println(TAG + "Waiting for termination for Sample "+currentSample);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				if (jCloudsNova.getNumWorkersCreated() == 0) {
					waitingForTerminationCounter++;
					waitingForTermination = false;
				}

				if (waitingForTerminationCounter > 10) {
					waitingForTerminationCounter = 0;
					jCloudsNova.terminateAll();
				}
			}

			System.out.println(TAG + "Termination finished for Sample "+currentSample);



			currentSample++;

			if(currentSample > sampleSize) {
				finished.add(currentPerformanceEvaluation);
				printAllResults();

				if (!queue.isEmpty()) {
					System.out.println(TAG+" Starting next test");
					new Thread(queue.poll()).start();
				}
				else {
					System.out.println(TAG+" ALL FINISHED");
					printAllResults();
				}

				running = false;
			}

		}

	}

	public static void printAllResults() {
		finished.forEach(PerformanceEvaluation::printResults);
	}

	public void printResults() {
		System.out.println("\n\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		System.out.printf("%s - Sample size: %d - dealine: %d - numJobs: %d - jobTemplates: %s %n", TAG, sampleSize, trialDeadlineModifier, trialNumJobs, arrayToString(trialJobTemplates));
		System.out.println(TAG+" - printing worker results");
		workerResults.forEach(results-> {
			System.out.println(TAG + " sample " + (workerResults.indexOf(results) + 1) + "--------------------------\n");
			results.forEach(System.out::println);
			System.out.println("\n");
		});
		System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n");
	}

	public static String arrayToString(String[] strArr) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < strArr.length; i++) {
			strBuilder.append(strArr[i]);
		}
		return strBuilder.toString();
	}

	public void notifyThread() {
		waiting = false;
		synchronized (lock) {
			lock.notify();
		}
	}

	public void addWorkerResults(ArrayList<String> results) {
		workerResults.add(results);
	}
}
