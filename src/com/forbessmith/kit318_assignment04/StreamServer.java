package com.forbessmith.kit318_assignment04;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;

/**
 * Created by nfs on 6/05/2017.
 */
public class StreamServer {
	// Server's listening port
	public static int port;
	private static int delay;

	// Server running
	private static volatile Boolean running = true;

	// Handles accepting all new connections and creates the public room
	// Creates a new thread to each client
	public static void main(String args[]) {
		if (args.length > 2) {
			try {
				port = Integer.parseInt(args[0]);
				if (port < 1024 || port > 65535) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException e) {
				System.err.println("Argument '" + args[0] + "' is an invalid port number - expecting an integer between 1024 and 65535");
				System.exit(-1);
			}

			try {
				delay = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				System.err.println("Argument '" + args[1] + "' is an invalid integer - for delay");
				System.exit(-1);
			}
			String textFileName = args[2];

			ServerSocket serverSocket = null;
			// Create server socket used to accept new connections from clients
			try {
				serverSocket = new ServerSocket(port);
				System.out.println("Server started");
			} catch (IOException e) {
				System.out.println("Server cannot listen on port "+port);
				running = false;
				System.exit(-1);
			}

			while(running){
				// Listen for new connections from clients
				try {
					Socket newClientSocket = serverSocket.accept(); // Wait and accept a connection
					System.out.println("Server connected to " + newClientSocket.getPort());

					StreamServerThread client = new StreamServerThread(newClientSocket, delay, textFileName);
					Thread clientThread = new Thread(client);
					clientThread.start();


				} catch (IOException e) {
					System.out.println("Server encountered an error accepting a connection");
				}

			}
		} else {
			System.out.println("Incorrect number of arguments given - 3 are needed: ");
			System.out.println("1 = port number to listen on (expecting an integer between 1024 and 49151)");
			System.out.println("2 = delay in ms between messages");
			System.out.println("3 = filename of text file");
			System.exit(-1);
		}

	}

}

