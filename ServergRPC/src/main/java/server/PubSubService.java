package server;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class PubSubService {

    private final String PROJECT_ID = "serious-fabric-252921";
    private final String TOPIC_NAME = "topicworkers";
    private final String SUBSCRIPTION_NAME = "subscription";
    private final String[] KEYS_MESSAGE = new String[]{"bucket", "file", "type", "size", "time"};
    private TopicAdminClient topicAdmin;


    public PubSubService(TopicAdminClient topicAdmin) {
        this.topicAdmin = topicAdmin;
    }

    public boolean publishMessage(String bucketName, String filename, String[] metadata) {
        TopicName tName = TopicName.ofProjectTopicName(PROJECT_ID, TOPIC_NAME);

        Publisher publisher = null;

        try {
            publisher = Publisher.newBuilder(tName).build();

            HashMap<String, String> map = new HashMap<>() {
                {
                    put(KEYS_MESSAGE[0], bucketName);
                    put(KEYS_MESSAGE[1], filename);
                    put(KEYS_MESSAGE[2], metadata[StorageService.INDEX_CONTENT_TYPE]);
                    put(KEYS_MESSAGE[3], metadata[StorageService.INDEX_SIZE]);
                    put(KEYS_MESSAGE[4], metadata[StorageService.INDEX_TIME_CREATED]);

                }
            };

            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().putAllAttributes(map).build();

            ApiFuture<String> future = publisher.publish(pubsubMessage);
            boolean sentMessage = false;
            String msgID = null;
            try {
                msgID = future.get();
                sentMessage = true;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            System.out.println("Message Published with ID=" + msgID);

            // At the end shutdown everything
            publisher.shutdown();
            return sentMessage;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
