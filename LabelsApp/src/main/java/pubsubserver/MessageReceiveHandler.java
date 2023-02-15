package pubsubserver;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.vision.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MessageReceiveHandler implements MessageReceiver {

    private final String[] MAP_KEYS = new String[] {"bucket", "file", "messageId", "type", "size", "time", "processedTime"};
    private final String PROJECT_ID = "serious-fabric-252921";
    private final String TOPIC_NAME = "translation";
    private final String SUBSCRIPTION_NAME = "subscription";
    private final String KEY_LABELS = "label";
    private TopicAdminClient topicAdmin;


    public MessageReceiveHandler(TopicAdminClient topicAdmin) {
        this.topicAdmin = topicAdmin;
    }

    public void receiveMessage(PubsubMessage msg , AckReplyConsumer ackReply) {
        String messageId = msg.getMessageId();

        Map<String, String> map =  msg.getAttributesMap();
        String bucket = map.get(MAP_KEYS[0]);
        String file = map.get(MAP_KEYS[1]);

        //list all the labels received
        List<String> labels = getLabels(bucket, file);
        String result = "";
        for(String label : labels) {
            result+= "Label: "+label+" | ";
        }
        System.out.println(result);


        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        Date date = new Date();

        String[] metadata = new String[] {messageId, map.get(MAP_KEYS[3]), map.get(MAP_KEYS[4]), map.get(MAP_KEYS[5]), ""+date};

        try {
            // don't need to create a topic or subscription when deploy a cloud function it's created automatically
            publishMessage(bucket, file, labels, metadata);
            //only acknowledges if published the message with success
            ackReply.ack();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> getLabels(String bucket, String file) {
        List<AnnotateImageRequest> imgRequests = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        String blobStorageURI = "gs://"+bucket+"/"+file;
        System.out.println(blobStorageURI);

        // gets the image with the blob URI
        Image img = Image.newBuilder().setSource(ImageSource.newBuilder().setImageUri(blobStorageURI).build()).build();

        // what will be done on the image, this case is label detections
        Feature feature = Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build();

        // creates the annotate request to get the features
        AnnotateImageRequest imgRequest = AnnotateImageRequest.newBuilder().addFeatures(feature).setImage(img).build();

        imgRequests.add(imgRequest);

        try(ImageAnnotatorClient client = ImageAnnotatorClient.create()) {
            BatchAnnotateImagesResponse response  = client.batchAnnotateImages(imgRequests);
            List<AnnotateImageResponse> responses = response.getResponsesList();

            for(AnnotateImageResponse res : responses) {
                if(res.hasError()) System.out.println("Error: "+res.getError().getMessage());

                else {
                    for(EntityAnnotation entAnnotation : res.getLabelAnnotationsList()) {
                        labels.add(entAnnotation.getDescription());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return labels;
    }

    private void publishMessage(String bucketName, String filename, List<String> labels, String[] metadata) throws IOException {
        TopicName tName = TopicName.ofProjectTopicName(PROJECT_ID, TOPIC_NAME);

        Publisher publisher = Publisher.newBuilder(tName).build();

        ByteString msgData = ByteString.copyFromUtf8(bucketName + ";"+filename);

        HashMap<String, String> map = new HashMap<>(){
            {
                put(MAP_KEYS[2], metadata[0]);
                put(MAP_KEYS[3], metadata[1]);
                put(MAP_KEYS[4], metadata[2]);
                put(MAP_KEYS[5], metadata[3]);
                put(MAP_KEYS[6], metadata[4]);
            }
        };

        for(int i = 0; i < labels.size(); i++) {
            map.put(KEY_LABELS+i, labels.get(i));
        }

        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(msgData).putAllAttributes(map).build();

        ApiFuture<String> future = publisher.publish(pubsubMessage);
        String msgID = null;
        try {
            msgID = future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        publisher.shutdown();
    }
}
