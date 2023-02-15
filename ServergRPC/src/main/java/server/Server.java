package server;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;
import contractservice.*;
import contractservice.Void;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Scanner;

public class Server extends ContractServiceGrpc.ContractServiceImplBase {

    private static Storage storage;

    private static int svcPort = 8000;
    private static FirestoreService service;
    private static TopicAdminClient topicAdmin;
    private static PubSubService pubsub;


    public static void main(String[] args) {
        try {
            // Assumes the environment variable args[0]
            // To create the connection to the buckets
            storage = null;
            storage = getStorageCredentials(args).getService();


            // Assumes the environment variable args[1]
            // To create the connection to the Firestore
            service = new FirestoreService(storage);

            service.init(args.length > 1 ? args[1] : null);


            //set GOOGLE_APPLICATION_CREDENTIALS= < ServiceAccountroject.json>
            // create the pubsub topic
            topicAdmin = TopicAdminClient.create();

            pubsub = new PubSubService(topicAdmin);

            // to begin the gRPC server
            io.grpc.Server svc = ServerBuilder.forPort(svcPort).addService(new Server()).build();
            svc.start();

            System.out.println("Server started , listening on " + svcPort);
            /*Scanner scan = new Scanner(System.in);
            scan.nextLine();*/
            svc.awaitTermination();
            svc.shutdown();
            topicAdmin.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public StreamObserver<Content> sendFileBlocks(StreamObserver<Identifier> responseObserver) {
        return new ServiceReceiveBlocks(storage, responseObserver, pubsub);
    }

    @Override
    public void getAllLabelNames(Void request, StreamObserver<PortugueseLabels> responseObserver) {
        ArrayList<String> labels = service.getAllLabels();

        PortugueseLabels ptLabels = PortugueseLabels.newBuilder().addAllLabels(labels).build();

        responseObserver.onNext(ptLabels);
        responseObserver.onCompleted();
    }

    @Override
    public void getFilesWithCharacteristics(Characteristics request, StreamObserver<Identifier> responseObserver) {
        String firstDate = request.getFirstDate();
        String secondDate = request.getSecondDate();
        String characteristic = request.getCharacteristicsInformation();

        ArrayList<String> identStr = service.getCharacteristicWithDates(firstDate, secondDate, characteristic);
        for(int i = 0; i < identStr.size(); i++) {
            Identifier identif = Identifier.newBuilder().setIdentification(identStr.get(i)).build();
            responseObserver.onNext(identif);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getCharacteristics(Identifier request, StreamObserver<PortugueseLabels> responseObserver) {
        String ident = request.getIdentification();

        ArrayList<String> labels = service.getLabels(ident);
        ArrayList<String> translations = service.getTranslations(ident);


        PortugueseLabels labelsPortuguese = PortugueseLabels.newBuilder().addAllLabels(labels).addAllTranslations(translations).build();

        responseObserver.onNext(labelsPortuguese);
        responseObserver.onCompleted();
    }

    @Override
    public void getContentStored(Name request, StreamObserver<Content> responseObserver) {
        String bucketName = request.getBucketName();
        String blobName = request.getFilename();

        Bucket bucket = storage.get(bucketName);
        Blob blob = bucket.get(blobName);

        Content.Builder contentBuild = Content.newBuilder().setContentType(blob.getContentType());

        byte[] byteFromBlob = blob.getContent();
        byte[] buffer = new byte[1024];

        int index = 0;
        while(index < byteFromBlob.length) {

            int dif = index+1024 > byteFromBlob.length ? byteFromBlob.length-index : 1024;
            for(int i = index; i < index+dif; i++) {
                buffer[i-index] = byteFromBlob[i];
            }
            ByteString byteBuffer = ByteString.copyFrom(ByteBuffer.wrap(buffer, 0, dif));
            Content content = null;
            if(index == 0)
                content = contentBuild.setFileBlockBytes(byteBuffer).build();

            else
                content = Content.newBuilder().setFileBlockBytes(byteBuffer).build();


            index+= 1024;
            responseObserver.onNext(content);
        }
        responseObserver.onCompleted();

    }

    @Override
    public void getAllFilesFromStorage(Void request, StreamObserver<Name> responseObserver) {
        for (Bucket bucket : storage.list().iterateAll()) {
            for (Blob blob : bucket.list().iterateAll()) {
                String bucketName = bucket.getName();
                String blobName = blob.getName();
                if(service.checkExistingDocumentOnFirestore(bucketName, blobName)) {
                    Name name = Name.newBuilder().setBucketName(bucket.getName()).setFilename(blob.getName()).build();
                    responseObserver.onNext(name);
                }
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public void checkServerConnection(Void request, StreamObserver<Void> responseObserver) {
        Void response = Void.newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // sets the storage credentials
    private static StorageOptions getStorageCredentials(String[] args) throws IOException {
        GoogleCredentials credentials = null;
        StorageOptions storageOptions = null;

        GoogleCredentials credentialsPubSub = null;
        if (args.length == 0)
            storageOptions = StorageOptions.getDefaultInstance();
        else {
            credentials = GoogleCredentials.fromStream(new FileInputStream(args[0]));
            storageOptions = StorageOptions.newBuilder().setCredentials(credentials).build();
        }

        // if the variables is not well defined, it ends the program
        String projID = storageOptions.getProjectId();
        if (projID != null) System.out.println("Current Project ID:" + projID);
        else {
            System.out.println("The environment variable GOOGLE_APPLICATION_CREDENTIALS isn't well defined!!");
            System.exit(-1);
        }

        return storageOptions;
    }
}
