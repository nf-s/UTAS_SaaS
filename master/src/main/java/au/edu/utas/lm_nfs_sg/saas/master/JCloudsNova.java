package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.ServerCreated;
import org.jclouds.openstack.nova.v2_0.domain.ServerExtendedStatus;
import org.jclouds.openstack.nova.v2_0.domain.regionscoped.AvailabilityZone;
import org.jclouds.openstack.nova.v2_0.features.FlavorApi;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

public class JCloudsNova implements Closeable {

	// ------------------------------------------------------------------------
	// Static Properties
	// ------------------------------------------------------------------------
	public static final String TAG="<JCloudsNova>";

	// Nectar cloud config constants
	private static final String NECTAR_ENDPOINT = "https://keystone.rc.nectar.org.au:5000/v2.0/";
	private static final String NECTAR_REGION = "Melbourne";
	private static final String NECTAR_PROVIDER = "openstack-nova";

	// Instance constants
	public static final String DEFAULT_IMAGE_ID = "210b3c59-3238-4abf-9447-dffbcca5cd1b";
	private static final String DEFAULT_KEYPAIR_NAME = "KIT318";
	private static final String DEFAULT_SECURITY_GROUPS_NAME = "saas";
	private static final String DEFAULT_FLAVOUR_NAME = "m2.small";
	private static final String DEFAULT_AVAILABILITY_ZONE = "tasmania";

	private static final String WORKER_JAR_LOCATION = "worker/worker-1.0-all.jar";

	// Nectar cloud config properties
	private static String osTenantName;
	private static String osUsername;
	private static String credential;
	private static String identity;

	// Jclouds static properties
	private static Map<String, Flavor> availableImageFlavours;
	private static Set<AvailabilityZone> availableZones;
	private static Flavor defaultFlavour;

	private static Map<Flavor, ArrayList<Long>> instanceFlavourCreationTime;

	private static Boolean initiated = false;

	// ------------------------------------------------------------------------
	// Static Constructor
	// ------------------------------------------------------------------------

	static {
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
			System.out.println(TAG + "An error occurred accessing NectarCloud JSON configuration file \n Exiting program \n");
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

	// ID used for Nova API
	private String instanceId;
	private String instanceHostname;
	private Flavor instanceFlavour;

	private Calendar startCreateCalendar;
	private Calendar finishCreateCalendar;
	private Long timeTakenToCreate;

	// ------------------------------------------------------------------------
	// Instance Constructor
	// ------------------------------------------------------------------------

	JCloudsNova() {
        //getting object to access nectar cloud apis
        novaApi = ContextBuilder.newBuilder(NECTAR_PROVIDER)
                .endpoint(NECTAR_ENDPOINT)
                .credentials(identity, credential)
                .buildApi(NovaApi.class);

		serverApi = novaApi.getServerApi(NECTAR_REGION);

		if (!initiated) {
			retrieveAvailableFlavours();
			retrieveAvailableRegions();
			initiated = true;
		}
    }

	// ------------------------------------------------------------------------
	// Initiation Methods
	// ------------------------------------------------------------------------

	private void retrieveAvailableFlavours() {
		availableImageFlavours = new HashMap<>();

		FlavorApi flavors = novaApi.getFlavorApi(NECTAR_REGION);
		for (Flavor flav : flavors.listInDetail().concat()) {
			// This insures that only non-legacy flavours are included
			if (!flav.getId().equals("") && flav.getName().startsWith("m2")) {
				System.out.println(getTag() + " Added flavour - "+ flav.toString());
				availableImageFlavours.put(flav.getName(), flav);
				if (flav.getName().equals(DEFAULT_FLAVOUR_NAME)) {
					System.out.println(getTag() + " Default flavour - "+ flav.toString());
					defaultFlavour = flav;
				}
			}
		}
	}

	private void retrieveAvailableRegions() {
		availableZones = novaApi.getAvailabilityZoneApi(NECTAR_REGION).get().listAvailabilityZones().toSet();
		availableZones.forEach((s -> System.out.printf("%s Added zone - %s%n", getTag(), s)));
	}

	// ------------------------------------------------------------------------
	// Worker Methods
	// ------------------------------------------------------------------------

	Boolean createWorker(String workerId, Boolean sharedWorker) {
		return createWorker(workerId, DEFAULT_IMAGE_ID, defaultFlavour, sharedWorker);
	}

	Boolean createWorker(String workerId, Flavor flavour, Boolean sharedWorker) {
		return createWorker(workerId, DEFAULT_IMAGE_ID, flavour, sharedWorker);
	}

    Boolean createWorker(String workerId, String imageId, Flavor flavour, Boolean sharedWorker){
		try {
			startCreateCalendar = Calendar.getInstance();

			System.out.println(getTag()+" Creating new cloud instance with flavour ="+flavour.getName());

			// Worker startup script:
			// Downloads worker-1.0-all.jar from web server (must be placed in 'worker' directory in src/.../webapps...)
			String startupScript="#!/bin/bash\nsudo su ubuntu\n"+
					"cd /home/ubuntu/saas \n"+
					"curl -o worker.jar "+Master.HOSTNAME+":"+Master.PORT+"/"+WORKER_JAR_LOCATION+" \n";

			//  WORKER ARGUMENTS (expects 4):
			//  1 - Worker id (string)
			//  2 - Master hostname (string)
			//  3 - Master socket port (integer between 1024 and 65535)
			//  4 - Shared worker (boolean - true = shared worker, false = unshared worker)
			startupScript = startupScript.concat(String.format("java -jar ./worker.jar %s %s %d %s \n ",
					workerId, Master.HOSTNAME, Master.PORT, sharedWorker.toString()));

			if (Master.DEBUG)
				System.out.printf("Startup Script: %n%s%n", startupScript);

			CreateServerOptions options1= CreateServerOptions.Builder.keyPairName(DEFAULT_KEYPAIR_NAME)
					.securityGroupNames(DEFAULT_SECURITY_GROUPS_NAME).userData(startupScript.getBytes()).availabilityZone(DEFAULT_AVAILABILITY_ZONE);

			ServerCreated server=serverApi.create(workerId,imageId,flavour.getId(),options1);

			instanceId = server.getId();
			instanceFlavour = flavour;

			close();

			return waitForInstance();

		} catch (Exception e) {
			System.out.println(getTag()+" An error occurred while creating new cloud instance");
			e.printStackTrace();

			return false;
		}

    }

	private Boolean waitForInstance() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		String instanceState = getInstanceState();
		int iterationCount = 1;

		while (instanceState.equals("BUILD")) {
			try {
				System.out.printf("%s waiting for server - current server status: %s (%d)%n", getTag(), instanceState, iterationCount);

				Thread.sleep(10000);

				iterationCount++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				instanceState = getInstanceState();
			} catch (Exception e) {
				instanceState = "FAILED";
			}
		}

		if (instanceState.equals("ACTIVE")) {
			instanceHostname = serverApi.get(instanceId).getAccessIPv4();

			System.out.printf("%s server successfully created!%n", getTag());
			System.out.println(getTag() + " server hostname = " + instanceHostname);

			return true;
		} else {
			System.out.println(getTag()+" could not create new cloud server - status = "+instanceState);
			return false;
		}
	}

	/**
	 *  Instance has been launched successfully.
	 *  This method calculates creation time and stores it to improve future creation time predictions
	 *
	 *  This method is called after the instance has been created AND successfully contacted through the Worker's Socket
	 *   - It is called from Worker.startWorkerSocketThread()
	 */
	void launchedSuccessfully() {
    	finishCreateCalendar = Calendar.getInstance();
    	timeTakenToCreate = finishCreateCalendar.getTimeInMillis()-startCreateCalendar.getTimeInMillis();

		System.out.println(getTag()+" took "+(timeTakenToCreate/1000)+" seconds to create");

    	if (instanceFlavourCreationTime == null) {
			instanceFlavourCreationTime = new HashMap<>();
		}

		if (!instanceFlavourCreationTime.containsKey(instanceFlavour)) {
			ArrayList<Long> newCreationTimesArray = new ArrayList<>();
			newCreationTimesArray.add(timeTakenToCreate);

    		instanceFlavourCreationTime.put(instanceFlavour, newCreationTimesArray);
		}
	}

	Boolean terminateServer() {
		serverApi.delete(instanceId);
		String serverTaskStatus = serverApi.get(instanceId).getExtendedStatus().get().getTaskState();

		if (serverTaskStatus.equals("deleting")) {
			System.out.println(getTag() + "Deleted server: " + instanceId);
			return true;
		} else {
			System.out.printf("%sFailed to delete server: %s status=%s%n", getTag(), instanceId, serverTaskStatus);
			return false;
		}
	}

	// ------------------------------------------------------------------------
	// Accessors & Cloud Instance "Accessors"
	// ------------------------------------------------------------------------

	String getInstanceHostname() {return instanceHostname;}

	Flavor getInstanceFlavour() {return instanceFlavour;}

	String getInstanceState() {
		return serverApi.get(instanceId).getStatus().value();
	}

    ServerExtendedStatus getInstanceExtendedStatus() {
		return serverApi.get(instanceId).getExtendedStatus().get();
	}

	String getTag() {
    	if(instanceHostname != null) {
			return String.format("%s [%s]", TAG, instanceHostname);
		} else {
    		return TAG;
		}
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
