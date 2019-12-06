import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class LocalApp {
    public static void main(String[] args) throws Exception {

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_WEST_2)
                .build();

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_WEST_2)
                .build();

        String input = args[0];

        List<String> inputs = new ArrayList<String>();

        List<Instance> runningInstances = new ArrayList<Instance>();
        while (!inputs.isEmpty()) {
            try {
                if (runningInstances.isEmpty() || noManager(runningInstances)) {
                    RunInstancesRequest request = new RunInstancesRequest()
                            .withImageId("ami-0c5204531f799e0c6")
                            .withInstanceType(InstanceType.T1Micro)
                            .withMaxCount(1)
                            .withMinCount(1);
                    request.setInstanceType(InstanceType.T2Micro.toString());
                }
                //List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            } catch (
                    AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
            }
        }

    }

    private boolean noManager(AmazonEC2 ec2) {

    }
}

