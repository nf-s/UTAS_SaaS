package au.edu.utas.lm_nfs_sg.saas.master_rest;

import au.edu.utas.lm_nfs_sg.saas.master.Job;
import au.edu.utas.lm_nfs_sg.saas.master.Master;

import com.google.gson.JsonParser;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * Created by nico on 4/09/2017.
 */
@Path("job")
public class JobResource {

	@Path("active")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getActiveJobs() {
		JSONObject returnObj = new JSONObject();
		returnObj.put("data", Master.getActiveJobsJSON());
		return Response.ok(returnObj.toJSONString()).build();
	}

	@Path("inactive")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getInactiveJobs() {
		JSONObject returnObj = new JSONObject();
		returnObj.put("data", Master.getInactiveJobsJSON());
		return Response.ok(returnObj.toJSONString()).build();
	}

	@Path("types")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJobTypes() {
		JSONObject returnObj = new JSONObject();
		returnObj.put("job-types", Job.jobTypes.keySet());
		return Response.ok(returnObj.toJSONString()).build();
	}

	@Path("{id}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJob(@PathParam("id") String jobId) {
		JSONObject responseObj = new JSONObject();

		String jobJsonString= Master.getInactiveJobParamsJsonString(jobId);

		if (!jobJsonString.equals("")) {
			try {
				responseObj.put("form_data", new JSONParser().parse(jobJsonString));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		responseObj.put("files_uploaded", Master.getInactiveJobResourcesDirFilenames(jobId));

		return Response.ok(responseObj.toJSONString()).build();
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Response createNewJob() {
		JSONObject returnObj = new JSONObject();
		returnObj.put("job-id", Master.createJob().getId());
		return Response.ok(returnObj.toJSONString()).build();
	}

	@Path("{id}")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	//@DefaultValue("") @FormDataParam("data")
	public Response updateJob(@PathParam("id") String jobId, String jsonRequest) {
		if (Master.setInactiveJobParamsJsonString(jobId, jsonRequest)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}


	// https://www.geekmj.org/jersey/jax-rs-multiple-files-upload-example-408/
	@Path("{id}/file")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@PathParam("id") String jobId,
							   @FormDataParam("file") InputStream file,
							   @FormDataParam("file") FormDataContentDisposition fileDisposition) {

		String fileName = fileDisposition.getFileName();

		File jobResDir = Master.getInactiveJobResourcesDir(jobId);

		saveFile(file, fileName, jobResDir);

		String fileDetails = " File uploaded successfully: " + fileName;

		System.out.println(fileDetails);

		return Response.ok(fileDetails).build();
	}

	@Path("{id}/files")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFiles2(@PathParam("id") String jobId,
								 @FormDataParam("files") List<FormDataBodyPart> bodyParts,
								 @FormDataParam("files") FormDataContentDisposition fileDispositions) {

		StringBuffer fileDetails = new StringBuffer("");

		File jobResDir = Master.getInactiveJobResourcesDir(jobId);

		/* Save multiple files */

		for (int i = 0; i < bodyParts.size(); i++) {
			/*
			 * Casting FormDataBodyPart to BodyPartEntity, which can give us
			 * InputStream for uploaded file
			 */
			BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyParts.get(i).getEntity();
			String fileName = bodyParts.get(i).getContentDisposition().getFileName();

			saveFile(bodyPartEntity.getInputStream(), fileName, jobResDir);

			fileDetails.append(" File uploaded successfully: ").append(fileName).append("<br/>");
		}

		System.out.println(fileDetails);

		return Response.ok(fileDetails.toString()).build();
	}

	private void saveFile(InputStream file, String name, File dir) {
		try {
			/* Change directory path */
			java.nio.file.Path path = FileSystems.getDefault().getPath(dir.getPath()+"/"+name);
			/* Save InputStream as file */
			Files.copy(file, path, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException ie) {
			ie.printStackTrace();
		}
	}

	@POST
	@Path("{id}/files/form_data")
	@Produces(MediaType.APPLICATION_JSON)
	public Response xmlToJson(@PathParam("id") String jobId) {
		JSONObject responseObj = new JSONObject();

		JSONObject formDataFromFile = Master.processInactiveJobResourcesDir(jobId);
		if (formDataFromFile!=null) {
			responseObj.put("form_data", formDataFromFile);
		}

		responseObj.put("files_uploaded", Master.getInactiveJobResourcesDirFilenames(jobId));

		return Response.ok(responseObj.toJSONString()).build();
	}

	@Path("{id}/launch")
	@PUT
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response launchJob(@PathParam("id") String jobId) {
		if (Master.initJob(jobId)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

	@Path("{id}/stop")
	@PUT
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response stopJob(@PathParam("id") String jobId) {
		System.out.println();
		if (Master.stopJob(jobId)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

}
