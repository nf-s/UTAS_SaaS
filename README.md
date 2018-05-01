# SparkCloud

## To get started
**Four Modules:**
+ Master
+ Worker
+ Comms (Socket communication library)
+ Common
    + Worker status and Job status enums
    + Worker type and Job type enums


### Dependencies
Gradle should handle everything.  
I recommend using IntelliJ, as Gradle will download required dependencies on project import.

### NectarCloud Credentials (JSON File)  
Modify properties in `master/src/main/resources/nectarcloud_config - SAMPLE.json`
      
      {
          "osTenantName": "",
          "osUsername": "",
          "credential": ""
      }
      
**osTenantName**  
Is the NectarCloud Project Name
        
**Credentials**  
Settings -> Reset Password

Then rename file to `nectarcloud_config.json`

### NectarCloud Configuration (in JCloudsNova.java)
NectarCloud Keypair name  
DEFAULT_KEYPAIR_NAME = "KIT318";

**Security Group**  
The security group must have inbound TCP traffic allowed for the post specified for the Master REST API (in *Set Hostname/Port of Master*).
As this port is used for the Java Socket server running on the Worker node.  

*Example Security Group*

    Egress	IPv6	Any	Any	        ::/0  
    Egress	IPv4	Any	Any	        0.0.0.0/0  
    Ingress	IPv4	TCP	22 (SSH)	0.0.0.0/0  
    Ingress	IPv4	TCP	80 (HTTP)	0.0.0.0/0  
    Ingress	IPv4	TCP	443 (HTTPS) 0.0.0.0/0  
    Ingress	IPv4	TCP	8081	    0.0.0.0/0  
    Ingress	IPv4	TCP	8443	    0.0.0.0/0  
       
DEFAULT_SECURITY_GROUPS_NAME = "saas";

Default Image ID used when creating new instances  
`DEFAULT_IMAGE_ID = "210b3c59-3238-4abf-9447-dffbcca5cd1b";`
  
`NECTAR_ENDPOINT = "https://keystone.rc.nectar.org.au:5000/v2.0/";`

NectarCloud API Region  
`NECTAR_REGION = "Melbourne";`

`DEFAULT_FLAVOUR_NAME = "m2.small";`

Largest flavour which can be created  
`LARGEST_FLAVOUR_NAME = "m2.large";`

Default region where instances are launched  
`DEFAULT_AVAILABILITY_ZONE = "tasmania";`

### Gradle Tasks
+ `Worker:shadowJar`
    + Builds a 'fat jar' of worker module, which contains dependencies needed (excluding Spark)
+ `Worker:moveToPublicFolder`
    + Moves the 'fat jar' to a public folder which is accessible from worker nodes.
    + Newly created workers will download the latest jar from this location
    
+ `Master:buildAllProducts`
    + Builds all components of master module using Gretty plugin
        + https://akhikhl.github.io/gretty-doc/Feature-overview.html
    + This includes web servlets, web server starter ...   
    + The Master server executables are located in `Master/build/output/master `
        + `start.bat`, `start.sh`
        + `stop.bat`, `stop.sh`...
        + all dependencies are included
    
+ `Master:startInplace`, `Master:stopInplace`, ...
    + Can be used for running Master in IDE
    + The server will automatically update as changes are made to the code
    
    
### Set Hostname/Port of Master
The hostname and port of the master node must be set at the start of `master/src/main/java/.../Master.java`

    public static final String HOSTNAME = "144.6.225.200";
    public static final int PORT = 8081;
    
It must also be set in `master/src/main/webapp/index.js` for the web client

    const MASTER_API_ROOTURL = "http://144.6.225.200:8081/api/client/";


### Configuration of Spark
The location of spark can be updated in `worker/src/main/java/.../job/Job.java`
Currently it is:

    /opt/csiro.au/spark_batch_fsp/bin/spark-batch
    

### Known Issues
Large file uploads using web client can timeout (without HTTP response). This is due to a bug in Jetty web server.

### Tips
+ NectarCloud defualt user is ubuntu
+ Use PuttyGen to convert private key from .pem to .ppk
+ Use WinSCP to transfer files
