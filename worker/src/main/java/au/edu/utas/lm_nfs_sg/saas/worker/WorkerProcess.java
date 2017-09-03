package au.edu.utas.lm_nfs_sg.saas.worker;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketClient;
import au.edu.utas.lm_nfs_sg.saas.comms.SocketServer1To1;

public class WorkerProcess {
	static final String TAG = "WorkerProcess";

	private volatile KFrequentCount frequentCountAlgorithm;
	private SocketClient socketClient;
	private SocketServer1To1 socketServer;

	private String jobId;
	private int kFreqWords;
	private String streamHostname;
	private int streamPort;

	private WorkerProcess(String jId, int k, String sHostname, int sPort) {
		jobId = jId;
		kFreqWords = k;

		streamHostname = sHostname;
		streamPort = sPort;
	}

	public static void main(String[] args) {
		if (args.length >= 4) {
			int streamPort = 0;
			int num_freq_words = 0;
			try {
				streamPort = Integer.parseInt(args[3]);
			} catch (NumberFormatException e) {
				System.out.println("Argument '" + args[3] + "' is an invalid port number");
				System.exit(-1);
			}

			try {
				num_freq_words = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				System.out.println("Argument '" + args[1] + "' is an integer - for number of frequent words");
				System.exit(-1);
			}

			String jobId = args[0];
			String streamHostname = args[2];

			WorkerProcess workerProcess = new WorkerProcess(jobId, num_freq_words, streamHostname, streamPort);
			workerProcess.run();

		} // If less than 3 arguments are given when executing the program
		else {
			System.out.println("Incorrect number of arguments given");
			System.exit(-1);
		}

		System.out.println(TAG + " Worker process exit");
	}

	private void run() {
		/*
		 * Create a socket client which connects to provided stream server
		 * Whenever a message is received from the stream server - it is passed to a Frequent Algorithm object
		 */
		frequentCountAlgorithm = new KFrequentCount(kFreqWords);

		// Connect to stream sever
		socketClient = new SocketClient(TAG+" StreamServerClient",streamHostname, streamPort);
		socketClient.setOnMessageReceivedListener( message -> {
			//System.out.println(socketClient.getTag() + " received "+message);
			frequentCountAlgorithm.frequentAlgorithm(message);
		});

		socketClient.setConnectionRetries(0);
		socketClient.setConnectionRetryDelayMs(1000);

		Thread socketClientThread = new Thread(socketClient);
		socketClientThread.start();
		socketServer = new SocketServer1To1(TAG+" MasterServer",0);

		socketServer.setOnMessageReceivedListener(message -> {
			System.out.println(socketServer.getTag() + " received "+message);

			switch (message) {
				case "results_please":
					frequentCountAlgorithm.sendResults(socketServer);
					break;
				case "finish_job":
					socketServer.sendMessage("job_finished");
					System.out.println(TAG+" is stopping");
					socketClient.stopRunning();
					socketServer.stopRunning();
					break;
			}
		});

		Thread socketServerThread = new Thread(socketServer);
		socketServerThread.start();

		Boolean socketServerIsRunning = socketServer.isRunning();

		while (!socketServerIsRunning) {
			try {
				Thread.sleep(100);
				socketServerIsRunning = socketServer.isRunning();
				if (!socketServerIsRunning)
					System.out.println(TAG + " Waiting for socket server to start");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Send port to stdout - which is captured by the Worker and sent to master
		try {
			System.out.println("result_listening_port "+jobId+" "+ socketServer.getPort());
		} catch (Exception e) {
			System.out.println("result_listening_port null");
		}

		try {
			socketClientThread.join();
			socketServerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println(TAG+" has stopped");
	}

	/**
	 * Creates a socket server which allows Master to connect and retrieve results
	 */
	private void createMasterSocketServer() {

	}
}
