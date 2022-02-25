import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();


        final Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties"))
        {
            properties.load(input);
        }
        final Collection<StepConfig> steps = new LinkedList<>();
//        steps.add(new StepConfig("EMR with combiners", new HadoopJarStepConfig("s3://" + properties.getProperty("bucketName") + "/" + properties.getProperty("jarFileName") + ".jar")
//                .withArgs("s3://" + properties.getProperty("bucketName") + "/withCombiners/",
//                        Boolean.toString(true) //with combiners
//                        )));

        steps.add(new StepConfig("EMR without combiners", new HadoopJarStepConfig("s3://" + properties.getProperty("jarBucketName") + "/" + properties.getProperty("jarFileName") + ".jar")
                .withArgs("s3://" + properties.getProperty("bucketName") + "/withoutCombiners/",
                        Boolean.toString(false) // without combiners
                        )));

        System.out.println("Cluster created with ID: " + AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build()
                // create the cluster
                .runJobFlow(new RunJobFlowRequest()
                        .withName("Knowledge-base for Word Prediction")
                        .withReleaseLabel("emr-6.4.0") // specifies the EMR release version label, we recommend the latest release
                        // create a step to enable debugging in the AWS Management Console
                        .withSteps(steps)
                        .withLogUri("s3://" + properties.getProperty("bucketName") + "/logs") // a URI in S3 for log files is required when debugging is enabled
                        .withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
                        .withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
                        .withInstances(new JobFlowInstancesConfig()
                                .withInstanceCount(3)
                                .withKeepJobFlowAliveWhenNoSteps(false)
                                .withMasterInstanceType(InstanceType.M4Large.toString())
                                .withSlaveInstanceType(InstanceType.M4Large.toString())))
                .getJobFlowId());
    }


}
