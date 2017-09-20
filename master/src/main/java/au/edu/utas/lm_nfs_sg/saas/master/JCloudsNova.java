package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.common.io.Closeables;
import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.domain.Flavor;
import org.jclouds.openstack.nova.v2_0.domain.ServerCreated;
import org.jclouds.openstack.nova.v2_0.features.FlavorApi;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.openstack.nova.v2_0.options.CreateServerOptions;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;

public class JCloudsNova implements Closeable {
	public static final String TAG="<JCloudsNova>";
	private static final int MAX_VCPU_COUNT = 2;

	private final NovaApi novaApi;
    private final ServerApi serverApi;
    private String serverId;
    private int serverNumVCpus;

	JCloudsNova() {
		JSONParser parser = new JSONParser();
		String osTenantName, osUsername, credential;
		osTenantName = osUsername = credential = null;
 
        try {
			StringWriter writer = new StringWriter();
			IOUtils.copy(ClassLoader.getSystemClassLoader().getResourceAsStream("nectarcloud_config.json"), writer);

            JSONObject jsonObject = (JSONObject) parser.parse(writer.toString());

			osTenantName = (String) jsonObject.get("osTenantName");
			osUsername = (String) jsonObject.get("osUsername");
			credential = (String) jsonObject.get("credential");
 
        } catch (Exception e) {
            e.printStackTrace();
        }

        String provider = "openstack-nova";
        String identity = String.format("%1$s:%2$s", osTenantName, osUsername); //concat osTenantName and osUsername with a ':'

        //getting object to access nectar cloud apis
        novaApi = ContextBuilder.newBuilder(provider)
                .endpoint("https://keystone.rc.nectar.org.au:5000/v2.0/")
                .credentials(identity, credential)
                .buildApi(NovaApi.class);

		serverApi = novaApi.getServerApi("Melbourne");
    }


    String createWorker(String serverName, int vCpuCount) {
        try {
            String retString =  createServer(serverName, vCpuCount);
            close();
            return retString;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String createServer(String nameser, int vCpuCount){
		System.out.println(TAG+" Creating new cloud instance");
		if (vCpuCount > MAX_VCPU_COUNT) {
			vCpuCount = MAX_VCPU_COUNT;
		}

		FlavorApi flavors=novaApi.getFlavorApi("Melbourne");
		String imageid = "26e87817-068b-4221-85a6-e5658aaa12a3";

		//System.out.println("\n\n\n flavour name");
		String flavorid="0";
		for(Flavor img1:flavors.listInDetail().concat())
		{
			//System.out.println(img1.getId()+ " "+img1.getName()+" "+img1.getDisk());
			//image size greater than 15G
			if((img1.getDisk()>15)&&(img1.getVcpus()==vCpuCount)){
				serverNumVCpus = img1.getVcpus();
				flavorid=img1.getId();
				break;
			}
		}

		String userdata="#!/bin/bash \n  sudo su ubuntu \n "+
				"cd /home/ubuntu/as04 \n "+
				"java com.forbessmith.kit318_assignment04.worker.Worker 1234 \n ";

		CreateServerOptions options1= CreateServerOptions.Builder.keyPairName("KIT318")
				.securityGroupNames("open").userData(userdata.getBytes());

		ServerCreated screa=serverApi.create(nameser,imageid,flavorid,options1);

		serverId = screa.getId();
		return serverId;
    }

    Boolean terminateServer(String id) {
    	return true;
	}

    String getServerStatus() {
		return serverApi.get(serverId).getStatus().toString();
	}

	String getServerIp() {
		return serverApi.get(serverId).getAccessIPv4();
	}

	int getServerNumVCpus() {return serverNumVCpus;}

    public void close() throws IOException {
        Closeables.close(novaApi, true);
    }
}
