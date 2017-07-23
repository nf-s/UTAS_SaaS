package com.forbessmith.kit318_assignment04;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;

public class StreamServerThread implements Runnable {
	private volatile Boolean running = true;
	private String textFileName;
	private int delay;
	private Socket newClientSocket;

	StreamServerThread(Socket socket, int del, String txtFileName) {
		newClientSocket = socket;
		delay =del;
		textFileName = txtFileName;
	}

	@Override
	public void run() {
		running = true;
		OutputStream s1out = null;
		DataOutputStream dos = null;
		try {
			s1out = newClientSocket.getOutputStream();
			dos = new DataOutputStream(s1out);
		} catch (IOException e) {
			running = false;
			e.printStackTrace();
		}

		BufferedReader br = null;
		StringBuilder sb = null;
		String line = null;
		try {
			br = new BufferedReader(new FileReader(textFileName));
			sb = new StringBuilder();
			line = br.readLine();
		} catch (IOException e){
			System.out.println("Server encountered an error reading text file");
			e.printStackTrace();
			running = false;
		}

		while(running) {
			try
			{
				// Loop through lines of text file
				while (line != null) {
					sb.append(line);
					sb.append(System.lineSeparator());
					// Loop through words of each line
					for (String aValue: line.replaceAll("\n ", " ").split(" ")) {
						// Every <delay> ms send word to server

						String word = aValue.replaceAll("[^\\p{L}\\p{Z}]","");
						if (!word.equals("")) {
							dos.writeUTF(word);
							Thread.sleep(delay);
						}

					}
					line = br.readLine();
				}

				if (line == null) {
					running = false;
					System.out.println("End of Text File");
				}
			}
			catch (SocketException e)
			{
				System.out.println("Client Disconnected from Server");
				running = false;
			}
			catch (Exception e)
			{
				System.out.println("Server encountered an error while sending message to client");
				running = false;
				e.printStackTrace();
			}
		}

		try {
			newClientSocket.close();
			dos.close();
			br.close();
		} catch (Exception e){
			System.out.println("Server encountered an error closing text file reader");
			e.printStackTrace();
		}
	}
}
