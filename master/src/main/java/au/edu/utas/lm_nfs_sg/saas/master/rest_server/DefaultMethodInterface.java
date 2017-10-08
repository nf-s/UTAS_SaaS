package au.edu.utas.lm_nfs_sg.saas.master.rest_server;


import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Example interface containing resource methods in form of Java8's default methods.
 *
 * @author Adam Lindenthal (adam.lindenthal at oracle.com)
 */
public interface DefaultMethodInterface {

	@GET
	default String root() {
		return "interface-root";
	}

	@GET
	@Path("path")
	default String path() {
		return "interface-path";
	}
}