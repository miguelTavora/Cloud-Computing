package pubsubserver;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.util.*;

public class ServerSubscriber {

    private static final String PROJECT_ID = "serious-fabric-252921";
    private static final String SUBSCRIPTION_NAME = "subscription";
    private static final String TOPIC_NAME = "topicworkers";
    private static TopicAdminClient topicAdmin;

    public static void main(String[] args) {
        ComputeEngineCredentials CompEngCr = ComputeEngineCredentials.create();

        try {
            topicAdmin = TopicAdminClient.create();

            requestCreationTopicAndSubscription();

            //prints all the subscriptions existing
            System.out.println(getAllSubscriptions());

            ProjectSubscriptionName projsubscriptionName = ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_NAME);
            Subscriber subscriber = Subscriber.newBuilder(projsubscriptionName, new MessageReceiveHandler(topicAdmin)).build();
            subscriber.startAsync().awaitRunning();



            System.out.println("Receiving messages");
            // to stop until enter
            /*Scanner scan = new Scanner(System.in);
            scan.nextLine();*/
            subscriber.awaitTerminated();

            subscriber.stopAsync();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    // tries maximum of 3 times to create the topic and subscription to the topic
    // if not success end the program
    private static void requestCreationTopicAndSubscription() {
        boolean success = false;
        int count = 0;
        while(!success) {
            // creates the topic and subscription if not exists (return false if didn't create subscription with success)
            success = createTopic();
            if(count == 2)
                throw new IllegalCallerException("Error trying to create the Topic and a subscription on Pub/Sub");
            count++;

        }
    }

    //creates the topic and a subscription automatically
    private static boolean createTopic() {
        boolean alreadyExists = false;
        TopicAdminClient.ListTopicsPagedResponse res = topicAdmin.listTopics(ProjectName.of(PROJECT_ID));

        //list all the topics existent
        for (Topic topic : res.iterateAll()) {
            String[] topicname = topic.getName().split("/");
            if (topicname[topicname.length - 1].equals(TOPIC_NAME)) {
                alreadyExists = true;
                break;
            }
        }

        // create the topic if not exists
        if (!alreadyExists) {
            // create the topic name
            TopicName tName = TopicName.ofProjectTopicName(PROJECT_ID, TOPIC_NAME);
            // create topic
            Topic topic = topicAdmin.createTopic(tName);

            return createSubscription(tName);
        }
        return true;
    }

    private static boolean createSubscription(TopicName tName) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_NAME);
        SubscriptionAdminClient subscriptionAdminClient = null;
        try {
            subscriptionAdminClient = SubscriptionAdminClient.create();

            // by default it's pull if want a push use the commented line below
            PushConfig pconfig = PushConfig.getDefaultInstance();
            //PushConfig.newBuilder.setPushEndpoint(ConsumerURL).build();

            Subscription subscription = subscriptionAdminClient.createSubscription(subscriptionName, tName, pconfig, 0);//default it's 10 seconds

            subscriptionAdminClient.close();
            if(subscription != null) {
                System.out.println("Subscription name: " + subscription.getName());
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // returns all the names of the subscriptions
    private static String getAllSubscriptions() {
        String result = "Subscription: ";

        try {
            SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create();

            ProjectName projectName = ProjectName.of(PROJECT_ID);

            for (Subscription sub : subscriptionAdminClient.listSubscriptions(projectName).iterateAll()) {
                String[] pathDivided = sub.getName().split("/");
                result+= pathDivided[pathDivided.length - 1]+" | ";
            }

            subscriptionAdminClient.shutdown();
            while(!subscriptionAdminClient.isShutdown()){
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
