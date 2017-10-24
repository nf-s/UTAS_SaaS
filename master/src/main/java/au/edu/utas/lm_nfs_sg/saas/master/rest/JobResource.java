package au.edu.utas.lm_nfs_sg.saas.master.rest;

import au.edu.utas.lm_nfs_sg.saas.master.Master;

import com.google.gson.*;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * Created by nico on 4/09/2017.
 */
public class JobResource {
	@Path("active")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getActiveJobs() {
		JsonObject returnObj = new JsonObject();
		returnObj.add("data", Master.getActiveJobsListJSON());
		return Response.ok(returnObj.toString()).build();
	}

	@Path("inactive")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getInactiveJobs() {
		JsonObject returnObj = new JsonObject();
		returnObj.add("data", Master.getInactiveJobsListJSON());
		return Response.ok(returnObj.toString()).build();
	}

	@Path("{id}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getJob(@PathParam("id") String jobId) {
		JsonObject responseObj = new JsonObject();

		String jobJsonString= Master.getJobConfigJsonString(jobId);

		if (!jobJsonString.equals("")) {
			responseObj.add("form_data", new JsonParser().parse(jobJsonString));
		}

		responseObj.add("files_uploaded", getDirFilenames(Master.getJobResourcesDir(jobId)));

		return Response.ok(responseObj.toString()).build();
	}

	@GET
	@Path("{id}/{folder}/{request}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDirectoryDetails(@PathParam("id") String jobId, @PathParam("folder") String folder, @PathParam("request") String request) {
		try {
			java.nio.file.Path jobDirectory = Master.getJobDir(jobId, folder);

			if (jobDirectory != null) {
				JsonObject returnObj = new JsonObject();
				switch (request) {
					case "filedetails":
						returnObj.add("data", getDirFileDetails(jobDirectory));
						break;
					case "filenames":
						returnObj.add("data", getDirFilenames(jobDirectory));
						break;
				}

				return Response.ok(returnObj.toString()).build();
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
		}

		return Response.status(Response.Status.NOT_FOUND).build();
	}

	private JsonArray getDirFilenames(java.nio.file.Path directory) {
		JsonArray filenames = new JsonArray();

		try {
			Files.list(directory)
					.filter(file->Files.isRegularFile(file))
					.forEach(file->filenames.add("./" + file.getFileName()));
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		return filenames;
	}

	private JsonArray getDirFileDetails(java.nio.file.Path directory) {
		JsonArray folderDetails = new JsonArray();

		try {
			Files.list(directory)
					.filter(file->Files.isRegularFile(file))
					.forEach(file-> {
						JsonObject fileDetails = new JsonObject();
						fileDetails.addProperty("filename", file.getFileName().toString());

						try {
							fileDetails.addProperty("size", humanReadableByteCount(Files.size(file), true));
						} catch (IOException e) {
							fileDetails.addProperty("size", "NA");
						}

						folderDetails.add(fileDetails);
					});
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		return folderDetails;
	}

	//https://stackoverflow.com/questions/12239868/whats-the-correct-way-to-send-a-file-from-rest-web-service-to-client?noredirect=1&lq=1
	@Path("{id}/{folder}/file/{filename}")
	@GET
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getFile(@PathParam("id") String jobId, @PathParam("filename") String filename, @PathParam("folder") String folder) {
		try {
			java.nio.file.Path foundFile = null;

			java.nio.file.Path jobDirectory = Master.getJobDir(jobId, folder);

			if (jobDirectory != null) {
				try {
					foundFile = Files.list(jobDirectory).filter(f-> f.getFileName().toString().equals(filename)).findFirst().orElse(null);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (foundFile != null && Files.exists(foundFile) && Files.isRegularFile(foundFile)) {
				Response.ResponseBuilder response = Response.ok(foundFile.toFile());
				response.header("Content-Disposition", String.format("attachment; filename=\"%s\"", filename));
				return response.build();
			}

		} catch (NullPointerException e) {
			e.printStackTrace();
		}

		return Response.status(Response.Status.NOT_FOUND).build();
	}


	@Path("{id}/config/file")
	@GET
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getConfigFile(@PathParam("id") String jobId) {
		try {
			java.nio.file.Path file = Master.getJobConfigFile(jobId);

			if (file != null && Files.exists(file) && Files.isRegularFile(file)) {
				Response.ResponseBuilder response = Response.ok(file.toFile());
				response.header("Content-Disposition", String.format("attachment; filename=\"%s\"", file.getFileName()));
				return response.build();
			}

		} catch (NullPointerException e) {
			e.printStackTrace();

		}

		return Response.status(Response.Status.NOT_FOUND).build();
	}

	// Job resources - Single File upload
	@Path("{id}/resources/file")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadSingleJobResourcesFile(@PathParam("id") String jobId,
										   @FormDataParam("file") InputStream file,
										   @FormDataParam("file") FormDataContentDisposition fileDisposition) {

		java.nio.file.Path jobResDir = Master.getJobResourcesDir(jobId);

		if (jobResDir != null) {
			return Response.ok(uploadSingleFile(file, fileDisposition, jobResDir)).build();
		}

		return Response.status(Response.Status.NOT_FOUND).build();
	}

	// Job resources - Multiple File upload
	@Path("{id}/resources/files")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadMultipleJobResourcesFiles(@PathParam("id") String jobId,
											  @FormDataParam("files") List<FormDataBodyPart> bodyParts,
											  @FormDataParam("files") FormDataContentDisposition fileDispositions) {

		java.nio.file.Path jobResDir = Master.getJobResourcesDir(jobId);

		if (jobResDir != null) {
			try {
				return Response.ok(uploadMultipleFiles(bodyParts, fileDispositions, jobResDir)).build();
			} catch (Exception e) {
				e.printStackTrace();
				return Response.serverError().build();
			}
		}

		return Response.status(Response.Status.NOT_FOUND).build();
	}

	// https://www.geekmj.org/jersey/jax-rs-multiple-files-upload-example-408/
	// Single file upload
	String uploadSingleFile(InputStream file, FormDataContentDisposition fileDisposition, java.nio.file.Path destinationDir) {
		String fileName = fileDisposition.getFileName();

		saveFile(file, fileName, destinationDir);

		return " File uploaded successfully: " + fileName;
	}

	// Multiple file upload
	String uploadMultipleFiles(List<FormDataBodyPart> bodyParts, FormDataContentDisposition fileDisposition, java.nio.file.Path destinationDir) throws Exception {

		StringBuffer fileDetails = new StringBuffer("");

		/* Save multiple files */

		for (int i = 0; i < bodyParts.size(); i++) {
			/*
			 * Casting FormDataBodyPart to BodyPartEntity, which can give us
			 * InputStream for uploaded file
			 */
			BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyParts.get(i).getEntity();
			String fileName = bodyParts.get(i).getContentDisposition().getFileName();

			saveFile(bodyPartEntity.getInputStream(), fileName, destinationDir);

			fileDetails.append(" File uploaded successfully: ").append(fileName).append("<br/>");
		}

		System.out.println(fileDetails);

		return fileDetails.toString();
	}

	private void saveFile(InputStream file, String name, java.nio.file.Path dir) {
		try {
			/* Change directory path */
			java.nio.file.Path path = FileSystems.getDefault().getPath(dir.toString(),name);
			/* Save InputStream as file */
			Files.copy(file, path, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException ie) {
			ie.printStackTrace();
		}
	}

	@POST
	@Path("{id}/resources/process_uploaded")
	@Produces(MediaType.APPLICATION_JSON)
	public Response processUploadedFiles(@PathParam("id") String jobId) {
		JsonObject responseObj = new JsonObject();

		JsonObject formDataFromFile = Master.processJobNewUploadedFiles(jobId);
		if (formDataFromFile!=null) {
			responseObj.add("form_data", formDataFromFile);
		}

		responseObj.add("files_uploaded", getDirFilenames(Master.getJobResourcesDir(jobId)));

		return Response.ok(responseObj.toString()).build();
	}

	//https://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
	public static String humanReadableByteCount(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit) return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

}
