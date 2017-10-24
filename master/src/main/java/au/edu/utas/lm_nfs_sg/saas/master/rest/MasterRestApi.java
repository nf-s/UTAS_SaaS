package au.edu.utas.lm_nfs_sg.saas.master.rest;

import au.edu.utas.lm_nfs_sg.saas.master.Master;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.ApplicationPath;

@WebListener
@ApplicationPath("api")
public class MasterRestApi extends ResourceConfig implements ServletContextListener {

	public MasterRestApi() {
		// Resources.
		register(MultiPartFeature.class);

		register(ClientJobResource.class);

		register(WorkerJobResource.class);
		register(WorkerResource.class);

		Master.init();

	}

	public void contextInitialized(ServletContextEvent sce) {
		//application is being deployed
	}
	public void contextDestroyed(ServletContextEvent sce) {
		System.out.println("SHUTDOWN");
		//Master.shutdown();
	}
}