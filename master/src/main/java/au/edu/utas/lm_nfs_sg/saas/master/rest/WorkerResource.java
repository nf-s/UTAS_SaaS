package au.edu.utas.lm_nfs_sg.saas.master.rest;

import au.edu.utas.lm_nfs_sg.saas.master.Master;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("worker")
public class WorkerResource {
	@Path("{id}/status")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response setWorkerStatus(@PathParam("id") String workerId, String jsonRequest) {
		System.out.println("got worker status");
		String newStatus = ((JsonObject) new JsonParser().parse(jsonRequest)).get("status").getAsString();

		if (newStatus != null) {
			if (Master.updateWorkerStatus(workerId, newStatus)) {
				return Response.ok().build();
			}
		}

		return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
	}
}
