package au.edu.utas.lm_nfs_sg.saas.master.rest_server;

import au.edu.utas.lm_nfs_sg.saas.master.Master;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("api")
public class MasterRestApi extends ResourceConfig {

	public MasterRestApi() {
		// Resources.
		register(MultiPartFeature.class);

		register(ClientJobResource.class);
		register(WorkerJobResource.class);

		Master.init();
	}
}