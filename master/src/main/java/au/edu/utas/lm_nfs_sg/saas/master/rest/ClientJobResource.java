package au.edu.utas.lm_nfs_sg.saas.master.rest;

import au.edu.utas.lm_nfs_sg.saas.master.Master;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("client/job")
public class ClientJobResource extends JobResource {

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Response createNewJob() {
		JsonObject returnObj = new JsonObject();
		returnObj.add("job-id", new JsonPrimitive(Master.createJob().getId()));
		return Response.ok(returnObj.toString()).build();
	}

	@Path("{id}")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	//@DefaultValue("") @FormDataParam("data")
	public Response updateJob(@PathParam("id") String jobId, String jsonRequest) {
		if (Master.updateJobConfig(jobId, jsonRequest)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

	@Path("{id}")
	@DELETE
	public Response deleteJob(@PathParam("id") String jobId) {
		if (Master.deleteJob(jobId)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

	@Path("{id}/launch")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response launchJob(@PathParam("id") String jobId, String jsonRequest) {
		JsonObject launchOptions = (JsonObject) new JsonParser().parse(jsonRequest);

		if (Master.activateJob(jobId, launchOptions)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

	@Path("{id}/stop")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response stopJob(@PathParam("id") String jobId) {
		if (Master.stopJob(jobId)) {
			return Response.ok().build();
		}
		return Response.status(Response.Status.NOT_FOUND).build();
	}

}
