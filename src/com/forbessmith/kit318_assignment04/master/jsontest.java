import org.json.*;

JSONObject obj = new JSONObject("nectarcloud_config.json");
String osTenantName = obj.getJSONObject("nectarCloudCredentials").getString("osTenantName");
String osUsername = obj.getJSONObject("nectarCloudCredentials").getString("osUsername");
String credential = obj.getJSONObject("nectarCloudCredentials").getString("credential");

System.out.println(osTenantName);