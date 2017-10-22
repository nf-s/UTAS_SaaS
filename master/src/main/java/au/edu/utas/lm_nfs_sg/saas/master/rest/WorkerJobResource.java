package au.edu.utas.lm_nfs_sg.saas.master.rest;

import au.edu.utas.lm_nfs_sg.saas.master.Master;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * Created by nico on 4/09/2017.
 */
@Path("worker/job")
public class WorkerJobResource extends JobResource{
	@Path("{id}/status")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response setJobStatus(@PathParam("id") String jobid, String jsonRequest) {
		String newStatus = ((JsonObject) new JsonParser().parse(jsonRequest)).get("status").getAsString();

		if (newStatus != null) {
			if (Master.updateJobStatusFromWorkerNode(jobid, newStatus)) {
				return Response.ok().build();
			}
		}

		return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
	}

	// Job Results - Single File upload
	@Path("{id}/results/file")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadSingleJobResultsFile(@PathParam("id") String jobId,
											   @FormDataParam("file") InputStream file,
											   @FormDataParam("file") FormDataContentDisposition fileDisposition) {

		File jobResDir = Master.getJobResultsDir(jobId);

		if (jobResDir != null) {
			return Response.ok(uploadSingleFile(file, fileDisposition, jobResDir)).build();
		}

		return Response.status(Response.Status.NOT_FOUND).build();
	}

	// Job Results - Multiple File upload
	@Path("{id}/results/files")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadMultipleJobResultsFiles(@PathParam("id") String jobId,
												  @FormDataParam("files") List<FormDataBodyPart> bodyParts,
												  @FormDataParam("files") FormDataContentDisposition fileDispositions) {

		File jobResDir = Master.getJobResultsDir(jobId);

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
}