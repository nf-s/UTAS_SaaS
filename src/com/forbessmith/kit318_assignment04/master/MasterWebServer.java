package com.forbessmith.kit318_assignment04.master;
import org.nanohttpd.protocols.http.IHTTPSession;
import org.nanohttpd.protocols.http.NanoHTTPD;
import org.nanohttpd.protocols.http.response.Response;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.nanohttpd.protocols.http.response.Response.newFixedLengthResponse;

public class MasterWebServer extends NanoHTTPD {

	MasterWebServer() throws IOException {
		super(8080);
		start(NanoHTTPD.SOCKET_READ_TIMEOUT, true);
		System.out.println("\nRunning! Point your browsers to http://localhost:8080/ \n");
	}

	private String htmlFileToString(String filename) {
		try {
			return new String(Files.readAllBytes(Paths.get("www/"+filename+".html")), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
	}

	@Override
	public Response serve(IHTTPSession session) {
		String msg = "";
		Map<String, String> parms = session.getParms();
		if (parms.get("action") == null) {
			msg = htmlFileToString("index_before") +htmlFileToString("index_after");
		} else {
			msg = htmlFileToString("index_before")+"<div class='message'>";
			switch (parms.get("action")) {
				case "new_job":
					try {
						msg += createMessageTitleDiv("Job request successfully created!");
						msg += "Job passcode<br/>";
						msg += Master.createFreqCountJob(Integer.parseInt(parms.get("job-kwords")), parms.get("job-stream-hostname"), Integer.parseInt(parms.get("job-stream-port")));
					} catch (Exception e) {
						msg = htmlFileToString("index_before")+"<div class='message'>"+createErrorMessageTitleDiv("An error occurred!");
						msg += "Could not create job.<p>You have provided incorrect information, please try again.</p>";
					}
					break;
				case "job_results":
					String jobResultsReturn = "";
					if (parms.get("update_results") == null) {
						jobResultsReturn = Master.getJobResults(parms.get("job-id"));
					} else {
						jobResultsReturn = Master.getJobResults(parms.get("job-id"), true);
					}

					switch (jobResultsReturn){
						case "incorrect_id":
							msg += createErrorMessageTitleDiv("Couldn't request results");
							msg += parms.get("job-id")+" isn't a correct <strong>active</strong> job passcode<br/>Your job could still be initiating.<br/>Please try again.";
							break;
						case "connection_timeout":
							msg += createErrorMessageTitleDiv("Couldn't connect to job");
							msg += "Sorry connection to job timed out.<br/>Please try again later.";
							break;
						case "success":
							msg += createMessageTitleDiv("Job results request successful!");
							msg += "Please below to retrieve results...<br/><a class='button_input_submit' href='http://localhost:8080/?action=job_results&job-id="+parms.get("job-id")+"'>Retrieve results</a>";
							break;
						default:
							msg += createMessageTitleDiv("Job results");
							msg += "<pre>"+jobResultsReturn+"</pre>";
							msg += "<br/><a class='button_input_submit' href='http://localhost:8080/?action=job_results&job-id="+parms.get("job-id")+"&update_results'>Request updated results</a>";
					}
					break;
				case "job_delete":
					String jobFinishReturn = Master.finishJob(parms.get("job-id"));

					switch (jobFinishReturn) {
						case "incorrect_id":
							msg += createErrorMessageTitleDiv("Couldn't stop job");
							msg += parms.get("job-id")+" isn't a correct <strong>active</strong> job passcode.<br/>Your job could still be initiating.<br/>Please try again.";
							break;
						case "connection_timeout":
							msg += createErrorMessageTitleDiv("Couldn't connect to job");
							msg += "Sorry connection to job timed out.<br/>Please try again later.";
							break;
						case "success":
							msg += createMessageTitleDiv("Job stop request successful!");
							msg += "Please click below to retrieve bill...<br/><a class='button_input_submit' href='http://localhost:8080/?action=job_delete&job-id=" + parms.get("job-id") + "'>Retrieve bill</a>";
							break;
						default:
							msg += createMessageTitleDiv("Job bill");
							msg += "<pre>" + jobFinishReturn + "</pre>";
					}
					break;
				case "admin":
					msg += createMessageTitleDiv("Admin status information");
					msg += "<pre>" + Master.printWorkerStatus() + "</pre>";
					msg += "<pre>" + Master.printJobStatus() + "</pre>";
					break;

			}
			msg += "</div></div>"+htmlFileToString("index_after");
		}


		return newFixedLengthResponse(msg);
	}

	private String createMessageTitleDiv(String title) {
		return "<div class='message_title'>"+title+"</div><div class='message_body'>";
	}

	private String createErrorMessageTitleDiv(String title) {
		return "<div class='message_title message_title_error'>"+title+"</div><div class='message_body'>";
	}
}