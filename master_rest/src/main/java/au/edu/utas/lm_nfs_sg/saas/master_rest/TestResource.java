package au.edu.utas.lm_nfs_sg.saas.master_rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path("test/{p}")
public class TestResource {
	@GET
	public String getLambdaResult(@PathParam("p") String p) {
		return p;
	}
}