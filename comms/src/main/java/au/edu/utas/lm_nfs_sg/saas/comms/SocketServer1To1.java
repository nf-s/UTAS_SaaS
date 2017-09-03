package au.edu.utas.lm_nfs_sg.saas.comms;

import java.io.*;
import java.net.ServerSocket;

public class SocketServer1To1 extends SocketCommunication {
	private MessageCallback callbackListener;

	public SocketServer1To1(String tag, int port) {
		super(tag, port);
	}

	public void run() {
		ServerSocket serverSocket = null;
		// Create server socket used to accept new connections from clients
		try {
			serverSocket = new ServerSocket(port);
			if (port == 0)
				port = serverSocket.getLocalPort();
			System.out.println(TAG +" Listening on port: " + port);
			running = true;
		} catch (IOException e) {
			System.out.println(TAG +" Server cannot listen on port "+port);
			running = false;
		}

		while(running){
			setStatus(Status.LISTENING);
			// Listen for new connections from clients
			try {
				sock = serverSocket.accept(); // Wait and accept a connection
				//messageReceivedListener.clientConnected();
				//serverSocket.close();
				System.out.println(TAG +" Server connected to " + sock.getPort());
				// Create new client thread for new socket connection
				socketConnected();
			} catch (IOException e) {
				System.out.println(TAG +" Server encountered an error accepting a connection");
			}
		}

		running = false;
		System.out.println(TAG+" stopped");
		setStatus(Status.STOPPED);
	}

	public interface MessageCallback extends MessageReceivedListener {
		//public void clientConnected();
		//public void clientDisconnected();
	}
}
