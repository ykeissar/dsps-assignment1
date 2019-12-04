import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class EC2Launch {
    public static void main(String[] args) throws Exception {
        ProfileCredentialsProvider profile = new ProfileCredentialsProvider();
        AWSCredentials  cred =profile.getCredentials() ;

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(cred);
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        AWSCredentialsProvider credentialsProvider2 = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        try {
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
            RunInstancesRequest request = new RunInstancesRequest("ami-8c1fece5", 1, 1);
            request.setInstanceType(InstanceType.T1Micro.toString());
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }
}

