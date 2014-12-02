package org.apache.s4.ddm;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;

public class EC2Manager {
	static Logger logger = LoggerFactory.getLogger(EC2Manager.class);

	private final AmazonEC2Client ec2;

	EC2Manager() throws IOException {
//		AWSCredentials credentials = new PropertiesCredentials(
//				EC2Manager.class
//						.getResourceAsStream("/ac.properties"));
		ec2 = new AmazonEC2Client(new DefaultAWSCredentialsProviderChain());
		ec2.setEndpoint("ec2.us-west-2.amazonaws.com");
	}

	public void startInstance(String strInstanceId) {
		StartInstancesRequest startRequest = new StartInstancesRequest()
				.withInstanceIds(strInstanceId);
		StartInstancesResult startResult = ec2.startInstances(startRequest);
		List<InstanceStateChange> stateChangeList = startResult
				.getStartingInstances();
		for (InstanceStateChange sc : stateChangeList) {
			logger.warn("Instance " + sc.getInstanceId() + ": before-"
					+ sc.getPreviousState().getName() + ", now-"
					+ sc.getCurrentState().getName());
		}
	}

}
