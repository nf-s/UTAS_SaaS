package au.edu.utas.lm_nfs_sg.saas.worker;

import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import javax.ws.rs.client.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.LinkedList;

class MasterRestClient {
	private final static String WORKER_API_KEY = "";
	private String TAG = "<MasterRestClient>";

	private static String masterRootUrl = "";
	private static String masterHostname = "";

	private Client client;

	MasterRestClient(String tag) {
		TAG +="["+tag+"]";

		client = ClientBuilder.newBuilder()
				.register(MultiPartFeature.class).build();
	}

	static void setMasterHostname(String masterH) {
		MasterRestClient.masterHostname = masterH;
		MasterRestClient.masterRootUrl = "http://"+masterH+":8081/api/worker/";
	}

	//================================================================================
	// Job REST "adapter" functions
	//================================================================================

	Boolean downloadJobConfigFile(String jobId, File destinationFolder) {
		String url = masterRootUrl +"job/"+jobId+"/config/file";
		return getFile(url, destinationFolder);
	}

	Boolean downloadJobResources(String jobId, File destinationFolder) {
		LinkedList<String> failedDownloads = new LinkedList<>();

		String filenamesJsonString = getJsonString(masterRootUrl +"job/"+jobId+"/resources/filenames");

		if (filenamesJsonString != null) {
			JsonArray filenames = new JsonParser().parse(filenamesJsonString).getAsJsonArray();

			filenames.forEach(filename->{
				// To get rid of './' at start of filename
				String[] filenameSplit = filename.getAsString().split("/");
				String filenameString = filenameSplit[filenameSplit.length-1];

				String url = masterRootUrl +"job/"+jobId+"/resources/file/"+filenameString;

				if (!getFile(url, destinationFolder)) {
					failedDownloads.add(filenameString);
				}
			});
		}

		if (failedDownloads.size() > 0) {
			return false;
		}
		return true;
	}

	Boolean uploadJobResultsFolder(String jobId, File sourceFolder) {
		Boolean ret = false;
		String url = masterRootUrl+"job/"+jobId+"/results/file";

		try {
			for (File f : sourceFolder.listFiles()) {
				ret = uploadFile(url, f);
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			return false;
		}

		return ret;
	}

	Boolean updateJobStatus(String jobId, JobControllerThread.Status  status) {
		return putJsonString(masterRootUrl+"job/"+jobId+"/status", "{\"status\":\""+status.toString()+"\"}");
	}

	//================================================================================
	// Generic REST functions
	//================================================================================

	private Boolean getFile(String url, File destinationFolder) {
		Response response = client.target(url).request()
				.accept(MediaType.APPLICATION_OCTET_STREAM)
				.get();

		if (response.getStatus() != 200) {
			System.out.printf("%s HTTP error: %d, URL: %s%n", TAG, response.getStatus(), url);
			return false;
		}

		String fileName = "";

		try {
			fileName = new ContentDisposition(response.getHeaderString(HttpHeaders.CONTENT_DISPOSITION)).getFileName();
		} catch (ParseException e) {
			e.printStackTrace();
			// Get filename from URL
			String[] filenameSplit = url.split("/");
			fileName = filenameSplit[filenameSplit.length-1];
		}

		System.out.printf("%s Downloading file %s, url %s %n", TAG, fileName, url);

		File destinationFile = new File(destinationFolder.getPath()+java.io.File.separator+fileName);

		InputStream in = response.readEntity(InputStream.class);
		if (in != null) {
			try {
				Files.copy(in, destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				return true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	private String getJsonString(String url) {
		System.out.printf("%s Getting JSON: %s%n", TAG, url);
		Response response = client.target(url).request()
				.accept(MediaType.APPLICATION_JSON)
				.get();

		if (response.getStatus() != 200) {
			System.out.printf("%s HTTP error: %d, URL: %s%n", TAG, response.getStatus(), url);
		}

		return response.readEntity(String.class);
	}

	private Boolean putJsonString(String url, String jsonString) {
		System.out.printf("%s Sending JSON: %s%n", TAG, url);
		WebTarget webTarget = client.target(url);

		Response response = webTarget.request("application/json")
				.put(Entity.json(jsonString));

		if (response.getStatus() != 200) {
			System.out.printf("%s HTTP error: %d, URL: %s%n", TAG, response.getStatus(), url);
			return false;
		}

		return true;
	}

	private Boolean uploadFile(String url, File file) {
		System.out.printf("%s Uploading: %s%n", TAG, url);

		WebTarget webTarget = client.target(url);
		MultiPart multiPart = new MultiPart();
		multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);

		FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("file",
				file, MediaType.APPLICATION_OCTET_STREAM_TYPE);
		multiPart.bodyPart(fileDataBodyPart);

		Response response = webTarget.request(MediaType.APPLICATION_JSON_TYPE)
				.post(Entity.entity(multiPart, multiPart.getMediaType()));

		System.out.printf("%s Successful upload - Response: %d %s%n URL: %s%n", TAG, response.getStatus(), response, url);

		if (response.getStatus() != 200) {
			System.out.printf("%s HTTP error: %d, URL: %s%n", TAG, response.getStatus(), url);
			return false;
		}

		return true;
	}
}
