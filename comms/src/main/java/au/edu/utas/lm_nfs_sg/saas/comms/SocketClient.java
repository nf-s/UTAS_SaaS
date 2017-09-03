package au.edu.utas.lm_nfs_sg.saas.comms;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

public class SocketClient extends SocketCommunication{
	private int connectionRetries = 4;
	private int connectionRetryDelayMs = 1000;

	private String hostname;

	public SocketClient(String tag, String h, int port) {
		super(tag, port);
		hostname = h;
	}

	public void setConnectionRetries(int r) {connectionRetries = r;}

	public void setConnectionRetryDelayMs(int r) {
		connectionRetryDelayMs = r;}

	public void run() {
		running = true;

		int connectionAttempt = 1;

		while (running) {
			setStatus(Status.CONNECTING);
			// Attempt to connect to worker
			try {
				sock = new Socket(hostname, port);
				System.out.println(TAG +" Connected to "+hostname+" port "+port);
				connected = true;
			}
			// Hostname is incorrect
			catch (UnknownHostException e) {
				System.out.println(TAG +" Hostname is incorrect");
				connected = false;
			}
			// If connection is unsuccessful - exit program
			catch (IOException e) {
				System.out.println(TAG +" Could not connect to server "+connectionAttempt);
				connected = false;
			}

			if (connected) {
				socketConnected(); // in SocketCommunication - main body of thread here
				closeSocket();
				connectionAttempt = 0;
			}

			if (connectionAttempt >= connectionRetries && connectionRetries!=0) {
				setStatus(Status.TIMEOUT);
				System.out.println(TAG +" Connection timeout");
				running = false;
			} else {
				try {
					Thread.sleep(connectionRetryDelayMs);
					connectionAttempt++;
				} catch (InterruptedException e) {
					e.printStackTrace();
					running = false;
				}
			}
		}


		running = false;
		System.out.println(TAG+" stopped");
		setStatus(Status.STOPPED);
	}

	public interface MessageCallback extends MessageReceivedListener {

	}
}
