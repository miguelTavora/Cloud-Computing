package functionhttp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;


import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

public class HttpIpProvider implements HttpFunction {

    private static final String PROJECT_NAME = "serious-fabric-252921";
    private static final String ZONE = "europe-west2-c";
    private static final String PARAMETER = "server";
    private static final String DEFAULT_SERVER_NAME = "instance-template-server";
    private static final Logger logger = Logger.getLogger(HttpIpProvider.class.getName());
    private static GoogleCredentials credentials = getComputeEngineCredentials();

    public static GoogleCredentials getComputeEngineCredentials() {
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void service(HttpRequest request, HttpResponse response) {
        String instanceName = request.getFirstQueryParameter(PARAMETER).orElse(DEFAULT_SERVER_NAME);

        ArrayList<String> ipAddresses = listVMInstances(PROJECT_NAME, ZONE, instanceName);
        if(ipAddresses != null) {
            String result = "";
            for (int i = 0; i < ipAddresses.size(); i++) {
                result += ipAddresses.get(i) + ";";
            }
            try {
                response.getOutputStream().write(result.getBytes());
                response.setStatusCode(200, result);
            } catch (IOException e) {
                e.printStackTrace();
                logger.info("Error sending the content to the client!");
                response.setStatusCode(500, result);
            }
        }
        else
            response.setStatusCode(503);
    }

    // to show the VM must be running
    public static ArrayList<String> listVMInstances(String project, String zone, String instanceName) {
        if(credentials == null) {
            logger.info("Error connecting to Compute Engine. Exiting function");
            throw new RuntimeException("Error connecting to Compute Engine");
        }

        ArrayList<String> ipAddresses = null;
        try {
            ipAddresses = new ArrayList<String>();
            try (InstancesClient client = InstancesClient.create()) {
                for (Instance e : client.list(project, zone).iterateAll()) {
                    if (e.getStatus() == Instance.Status.RUNNING) {
                        if (e.getName().startsWith(instanceName)) {
                            String ip = e.getNetworkInterfaces(0).getAccessConfigs(0).getNatIP();
                            ipAddresses.add(ip);
                            //logger.info("Name : " + e.getName() + "; IP: " + ip);
                        }
                    }
                }
            }
            return ipAddresses;
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("Error accessing the Compute Engine!");
        }
        return null;
    }
}
