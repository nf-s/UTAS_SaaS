package com.forbessmith.kit318_assignment04.socketcomms;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class SocketCommunication implements Runnable {
	public enum Status {
		CONNECTING, CONNECTED, DISCONNECTED, TIMEOUT, LISTENING, STOPPED
	}

	String TAG;

	int port;
	Boolean connected;

	Socket sock;
	private OutputStream outputStream;
	private DataOutputStream dataOutputStream;
	private InputStream inputStream;
	private DataInputStream dataInputStream;

	private MessageReceivedListener messageReceivedListener;
	private StatusChangeListener statusChangeListener;

	private volatile LinkedList<String> outputMessageStack;

	volatile Boolean running = false;

	SocketCommunication(String t, int p) {
		super();
		port = p;
		TAG = t;
		outputMessageStack = new LinkedList<>();
	}

	public abstract void run();

	public synchronized Boolean isRunning() {
		return running;
	}

	public synchronized Boolean isConnected() {
		return connected;
	}

	public String getTag() {
		return TAG;
	}

	void socketConnected() {
		inputStream = null;
		dataInputStream = null;

		try {
			inputStream = sock.getInputStream();
			dataInputStream = new DataInputStream(inputStream);
			connected = true;
			setStatus(Status.CONNECTED);
			System.out.println(TAG +" Socket connected");


		} catch (IOException e) {
			System.out.println(TAG +" Error occurred while getting input stream");
			connected = false;
		}

		while (connected) {
			try {

				Iterator<String> iterator = outputMessageStack.iterator();
				while(iterator.hasNext()) {
					String outputMessage = iterator.next();
					sendMessage(outputMessage);
					iterator.remove();
				}

				// Wait to receive message from server
				String st = dataInputStream.readUTF();
				if (messageReceivedListener != null)
					messageReceivedListener.newMessageReceived(st);

			} catch (SocketException e) {
				System.out.println(TAG +" Server lost connection with client");
				connected = false;
				closeSocket();
			} catch (IOException e) {
				System.out.println(TAG +" Server encountered an error reading message");
				connected = false;
				closeSocket();
			}
		}
	}

	public int getPort() {
		return port;
	}

	/*public synchronized void addMessageToStack(String mess) {
		outputMessageStack.add(mess);
	}*/

	public void sendMessage(String mess) {
		try {
			if (sock == null)
				outputMessageStack.add(mess);
			else {
				if (outputStream == null)
					outputStream = sock.getOutputStream();
				if (dataOutputStream == null)
					dataOutputStream = new DataOutputStream(outputStream);
				dataOutputStream.writeUTF(mess);
			}
		} catch (SocketException e) {
			outputMessageStack.add(mess);
			System.out.println(TAG +" Couldn't send message - Socket failed");
			closeSocket();
		} catch (IOException e2) {
			outputMessageStack.add(mess);
			System.out.println(TAG +" Couldn't send message");
			e2.printStackTrace(System.out);
		}
	}

	public void closeSocket() {
		setStatus(Status.DISCONNECTED);

		connected = false;
		try {
			if (sock!= null) {
				sock.close();
				sock = null;
			}

			if (inputStream != null) {
				inputStream.close();
				inputStream = null;
			}

			if (dataInputStream != null) {
				dataInputStream.close();
				dataInputStream = null;
			}

			if (outputStream != null) {
				outputStream.close();
				outputStream = null;
			}
			if (dataOutputStream != null) {
				dataOutputStream.close();
				dataOutputStream = null;
			}
		} catch (IOException | NullPointerException e2) {
			System.out.println(TAG +" Tried to cleanly close connection");
		}
	}

	public void stopRunning() {
		System.out.println(TAG+" is stopping");
		running = false;
	}

	public void setStatus(Status newStatus) {
		if (statusChangeListener != null)
			statusChangeListener.onStatusChanged(this, newStatus);
	}

	public void setOnMessageReceivedListener(MessageReceivedListener listener) {
		messageReceivedListener = listener;}

	public interface MessageReceivedListener {
		void newMessageReceived(String message);
	}

	public void setOnStatusChangeListener(StatusChangeListener listener) {
		statusChangeListener = listener;
	}

	public interface StatusChangeListener {
		void onStatusChanged(SocketCommunication socketCommunication, Status currentStatus) ;
	}
}
