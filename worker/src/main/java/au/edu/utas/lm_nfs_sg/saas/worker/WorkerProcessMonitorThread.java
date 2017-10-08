package au.edu.utas.lm_nfs_sg.saas.worker;

import java.util.*;

public class WorkerProcessMonitorThread implements Runnable{
	static final String TAG = "WorkerProcessMonitorThread";

	private volatile ArrayList<Process> processes;
	private volatile ArrayList<String> processCommands;

	private volatile Boolean running;

	WorkerProcessMonitorThread() {
		processes = new ArrayList<>();
		processCommands = new ArrayList<>();
		running = true;
	}

	@Override
	public void run() {
		while (running) {
			try {
				Thread.sleep(5000);
				checkProcesses();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	void stopThread() {
		running = false;
	}

	synchronized void addProcess(String command, Process process) {
		System.out.println(TAG+" added new process "+command);
		processes.add(process);
		processCommands.add(command);
	}

	private synchronized void checkProcesses() {
		System.out.println(TAG+" monitoring "+processes.size()+" processes");
		Iterator<Process> processIterator = processes.iterator();
		Iterator<String> processCommandIterator = processCommands.iterator();

		LinkedList<Integer> removeIndexes = new LinkedList<>();

		while(processIterator.hasNext() && processCommandIterator.hasNext()) {
			Process currentProcess = processIterator.next();
			String currentCommand = processCommandIterator.next();
			if (!currentProcess.isAlive()) {
				try {
					currentProcess.destroyForcibly();
				} catch (Exception e) {
					// already destroyed
				}
				System.out.println(TAG + " process is being restarted - " + currentCommand);
				removeIndexes.push(processCommands.indexOf(currentCommand));
			}
		}

		for (Integer index:removeIndexes) {
			String command = processCommands.get(index);
			//Worker.runProcessFromString(processCommands.get(index));
			System.out.println(TAG+ " process successfully restarted - " + command);

			processCommands.remove((int)index);
			processes.remove((int)index);
		}
	}

	synchronized Boolean removeProcess(String mess) {
		if (processCommands.contains(mess)) {
			processes.remove(processCommands.indexOf(mess));
			processCommands.remove(mess);
			System.out.println(TAG + " process is removed from array - " + mess);
			return true;
		}
		return false;
	}
}
