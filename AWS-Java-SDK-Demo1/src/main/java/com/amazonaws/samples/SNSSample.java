package com.amazonaws.samples;

import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.policy.Condition;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SNSActions;
import com.amazonaws.auth.policy.conditions.SNSConditionFactory;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeRequest;

public class SNSSample {

	private static AWSCredentials credentials;
	private static AmazonSNSClient client;

	public static void main(String[] args) throws IOException {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential profile
		 * by reading from the credentials file located at
		 * (C:\\Users\\Ray\\.aws\\credentials).
		 */

		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
			//credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (C:\\Users\\Ray\\.aws\\credentials), and is in valid format.", e);
		}

		client = new AmazonSNSClient(credentials);
		client.setRegion(Region.getRegion(Regions.US_EAST_2));

		String topicArn = createTopic("SDKTopic");
		setTopicAttribute(topicArn);
		
		subscribeTopic(topicArn);

		/*manually copy from email or in the topic console */
		String subscriptionArn = "arn:aws:sns:us-east-2:16008955xxx:SDKTopic:80ef54e3-ba29-4f57-96f8-af149bf7";
		/*
		 * subscribe email need to confirm before it can receive published email.
		 */

		String subject = "AWS SNS message through Eclipse SDK";
		String message = "This is a sample mail sent through Eclipse.\nRegards,\nRay";

		publishEmailMessage(topicArn, subject, message);

		unsubscribeTopic(subscriptionArn);

		deleteTopic(topicArn);

	}

	public static String createTopic(String topicName) {
		CreateTopicRequest topic = new CreateTopicRequest();
		topic.setName(topicName);
		topic.setRequestCredentials(credentials);
		CreateTopicResult topicRequestResult = client.createTopic(topic);
		String topicArn = topicRequestResult.getTopicArn();
		System.out.println(topicArn);
		// Will print topicArn as "arn:aws:sns:us-east-2:xxxxx:SDKTopic";
		return topicArn;
	}

	public static void deleteTopic(String topicArn) {
		DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(topicArn);
		deleteTopicRequest.setRequestCredentials(credentials);
		client.deleteTopic(deleteTopicRequest);
	}

	/*
	 * The following code will provide all the subscription permissions 
	 * to the endpoints that end with @infosys.com on SDKtopic
	 * 
	 * Exception: Policy statement must apply to a single resource
	 */
	public static void setTopicAttribute(String topicArn) {
		Condition endpointCondition = SNSConditionFactory.newEndpointCondition("*@infosys.com");
		Policy policy = new Policy().withStatements(new Statement(Effect.Allow).withPrincipals(Principal.AllUsers)
				.withResources(new Resource(topicArn))
				.withActions(SNSActions.Subscribe).withConditions(endpointCondition));
		client.setTopicAttributes(new SetTopicAttributesRequest(topicArn, "Policy", policy.toJson()));
	}

	public static String subscribeTopic(String topicArn) {
		SubscribeRequest subscribeRequest = new SubscribeRequest();
		subscribeRequest.setTopicArn(topicArn);
		subscribeRequest.setRequestCredentials(credentials);
		subscribeRequest.setEndpoint("xxx@gmail.com");
		subscribeRequest.setProtocol("email");
		String subscriptionArn = client.subscribe(subscribeRequest).getSubscriptionArn();
		System.out.println(subscriptionArn); //pending confirmation
		return subscriptionArn;
	}

	public static void publishEmailMessage(String topicArn, String subject, String message) {
		PublishRequest publishRequest = new PublishRequest();
		publishRequest.setSubject(subject);
		publishRequest.setMessage(message);
		publishRequest.setTopicArn(topicArn);
		publishRequest.setRequestCredentials(credentials);
		PublishResult publishResult = client.publish(publishRequest);
		System.out.println(publishResult.getMessageId());
	}

	public static void unsubscribeTopic(String subscriptionArn) {
		UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest();
		unsubscribeRequest.setRequestCredentials(credentials);
		unsubscribeRequest.setSubscriptionArn(subscriptionArn);
		client.unsubscribe(unsubscribeRequest);
	}

}
