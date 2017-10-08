package au.edu.utas.lm_nfs_sg.saas.master.rest_server;

import au.edu.utas.lm_nfs_sg.saas.master.Master;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("default-method")
@Produces("text/plain")
public class DefaultMethodResource implements DefaultMethodInterface {

	@GET
	@Path("class")
	public String fromClass() {
		return "class"+ Master.test();
	}
}