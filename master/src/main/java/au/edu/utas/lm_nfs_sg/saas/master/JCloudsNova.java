package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.ServerCreated;
import org.jclouds.openstack.nova.v2_0.features.FlavorApi;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class JCloudsNova implements Closeable {

	// ------------------------------------------------------------------------
	// Static Properties
	// ------------------------------------------------------------------------
	public static final String TAG="<JCloudsNova>";
	public static final String DEFAULT_IMAGE_ID = "210b3c59-3238-4abf-9447-dffbcca5cd1b";

	private static Map<String, Flavor> imageFlavours;
	private static Flavor defaultFlavour;

	private static Map<Flavor, ArrayList<Long>> instanceFlavourCreationTime;

	private static String endpoint;
	private static String region;

	private static String osTenantName;
	private static String osUsername;
	private static String credential;
	private static String provider;
	private static String identity;

    private static Boolean initiated = false;

	// ------------------------------------------------------------------------
	// Static Constructor
	// ------------------------------------------------------------------------

    static {
		provider = "openstack-nova";
		endpoint = "https://keystone.rc.nectar.org.au:5000/v2.0/";
		region = "Melbourne";

		JsonParser parser = new JsonParser();
		try {
			StringWriter writer = new StringWriter();
			IOUtils.copy(JCloudsNova.class.getClassLoader().getResourceAsStream("nectarcloud_config.json"), writer);

			JsonObject jsonObject = parser.parse(writer.toString()).getAsJsonObject();

			osTenantName = jsonObject.get("osTenantName").getAsString();
			osUsername = jsonObject.get("osUsername").getAsString();
			credential = jsonObject.get("credential").getAsString();

			identity = String.format("%1$s:%2$s", osTenantName, osUsername);
		} catch (Exception e) {
			System.out.println(TAG+ "An error occurred accessing NectarCloud JSON configuration file \n Exiting program \n");
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static Flavor getDefaultFlavour() {
		return defaultFlavour;
	}

	// ------------------------------------------------------------------------
	// Instance Properties
	// ------------------------------------------------------------------------

	private final NovaApi novaApi;
	private final ServerApi serverApi;

	private String instanceId;
	private Flavor instanceFlavour;

	private Calendar startCreateCalendar;
	private Calendar finishCreateCalendar;
	private Long timeTakenToCreate;

	// ------------------------------------------------------------------------
	// Instance Constructor
	// ------------------------------------------------------------------------

	JCloudsNova() {
        //getting object to access nectar cloud apis
        novaApi = ContextBuilder.newBuilder(provider)
                .endpoint(endpoint)
                .credentials(identity, credential)
                .buildApi(NovaApi.class);

		serverApi = novaApi.getServerApi(region);

		if (!initiated) {
			retrieveFlavourInfo();
			initiated = true;
		}
    }

	// ------------------------------------------------------------------------
	// Initiation Methods
	// ------------------------------------------------------------------------

	private void retrieveFlavourInfo() {
		imageFlavours = new HashMap<>();
		System.out.println(TAG + " Printing flavours new cloud instance");

		FlavorApi flavors = novaApi.getFlavorApi("Melbourne");
		for (Flavor flav : flavors.listInDetail().concat()) {
			// This insures that only non-legacy flavours are included
			if (!flav.getId().equals("")) {
				System.out.println(TAG + " Added - "+ flav.toString());
				imageFlavours.put(flav.getName(), flav);
				if (flav.getName().equals("m2.small")) {
					System.out.println(TAG + " Default flavour - "+ flav.toString());
					defaultFlavour = flav;
				}
			}
		}

	}

	// ------------------------------------------------------------------------
	// Worker Methods
	// ------------------------------------------------------------------------

	String createWorker(String workerId, Boolean sharedWorker) {
		return createWorker(workerId, DEFAULT_IMAGE_ID, defaultFlavour, sharedWorker);
	}

	String createWorker(String workerId, Flavor flavour, Boolean sharedWorker) {
		return createWorker(workerId, DEFAULT_IMAGE_ID, flavour, sharedWorker);
	}

    String createWorker(String workerId, String imageId, Flavor flavour, Boolean sharedWorker){
		try {
			startCreateCalendar = Calendar.getInstance();

			System.out.println(TAG+" Creating new cloud instance with flavour ="+flavour.getName());

			String startupScript="#!/bin/bash \n  sudo su ubuntu \n "+
					"cd /home/ubuntu/saas \n "+
					"curl -o worker.jar "+Master.HOSTNAME+":"+Master.PORT+"/worker/worker-1.0-all.jar \n" +
					"java -jar ./worker.jar "+Master.HOSTNAME+" "+Master.PORT+" "+sharedWorker.toString()+" \n ";

			CreateServerOptions options1= CreateServerOptions.Builder.keyPairName("KIT318")
					.securityGroupNames("saas").userData(startupScript.getBytes());

			ServerCreated server=serverApi.create(workerId,imageId,flavour.getId(),options1);

			instanceId = server.getId();
			instanceFlavour = flavour;

			close();
			return instanceId;
		} catch (Exception e) {
			System.out.println(TAG+" An error occurred while creating new cloud instance");
			e.printStackTrace();
			return null;
		}
    }

    Boolean terminateServer(String id) {
    	return true;
	}

	void createWasSuccessful() {
    	finishCreateCalendar = Calendar.getInstance();
    	timeTakenToCreate = finishCreateCalendar.getTimeInMillis()-startCreateCalendar.getTimeInMillis();

    	if (instanceFlavourCreationTime == null) {
			instanceFlavourCreationTime = new HashMap<>();
		}

		if (!instanceFlavourCreationTime.containsKey(instanceFlavour)) {
			ArrayList<Long> newCreationTimesArray = new ArrayList<>();
			newCreationTimesArray.add(timeTakenToCreate);

    		instanceFlavourCreationTime.put(instanceFlavour, newCreationTimesArray);
		}
	}

	// ------------------------------------------------------------------------
	// Accessors & Cloud Instance "Accessors"
	// ------------------------------------------------------------------------

	Flavor getInstanceFlavour() {return instanceFlavour;}

    String getServerStatus() {
		return serverApi.get(instanceId).getStatus().toString();
	}

	String getServerIp() {
		return serverApi.get(instanceId).getAccessIPv4();
	}

	// ------------------------------------------------------------------------
	// JCloud Methods
	// ------------------------------------------------------------------------

    public void close() throws IOException {
        Closeables.close(novaApi, true);
    }

	// ------------------------------------------------------------------------
	// Cloud Instance Creation Time Prediction
	// ------------------------------------------------------------------------
	public static Long estimateCreationTimeInMs() {return estimateCreationTimeInMs(defaultFlavour);}
	public static Long estimateCreationTimeInMs(Flavor instanceFlavour) {
    	Long returnEstimate;

    	// If the specified instance flavour has creation time data saved
		if (instanceFlavourCreationTime != null && !instanceFlavourCreationTime.containsKey(instanceFlavour)) {
			Long averageCreationTime = 0L;

			for(Long creationTime:instanceFlavourCreationTime.get(instanceFlavour)) {
				averageCreationTime += creationTime;
			}

			returnEstimate =  averageCreationTime/instanceFlavourCreationTime.get(instanceFlavour).size();
		}
		// Or return default time - 3 minutes
		else {
			returnEstimate = (long) 3 * 60 * 1000;
		}

		System.out.println(TAG+" Estimated time to create worker (flavour="+instanceFlavour.getName()+") is "+returnEstimate.toString()+" ms");

		return returnEstimate;
	}

}
