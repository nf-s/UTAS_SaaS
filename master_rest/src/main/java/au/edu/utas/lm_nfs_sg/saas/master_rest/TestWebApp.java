package au.edu.utas.lm_nfs_sg.saas.master_rest;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("test")
public class TestWebApp extends ResourceConfig {

	public TestWebApp() {
		// Resources.
		register(MultiPartFeature.class);

		register(DefaultMethodResource.class);
		register(TestResource.class);
		register(FileUpload.class);
	}
}