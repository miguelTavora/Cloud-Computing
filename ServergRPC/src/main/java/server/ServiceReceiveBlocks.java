package server;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import contractservice.Content;
import contractservice.Identifier;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class ServiceReceiveBlocks implements StreamObserver<Content> {

    // used to pass as argument on the content
    private Storage storage;
    private StorageService storageService;
    private String filename = null;
    private StreamObserver<Identifier> identificationStream;
    private PubSubService pubsub;

    public ServiceReceiveBlocks(Storage storage, StreamObserver<Identifier> replies, PubSubService pubsub) {
        this.storage = storage;
        this.identificationStream = replies;
        this.pubsub = pubsub;
    }

    @Override
    public void onNext(Content content) {
        if(this.filename == null) {
            String writtenName = content.getFilename();
            String contentType = content.getContentType();

            storageService = new StorageService(storage, writtenName, contentType);
            this.filename = storageService.getFilename();
        }
        //sets the bytes to the blob of the bucket
        storageService.uploadContentToBlob(content.getFileBlockBytes());
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Error receiving the blocks of the content");
    }

    @Override
    public void onCompleted() {
        storageService.closeUploadContent();

        String bucketName = storageService.getBucketName();

        String idBlob = bucketName+"_"+this.filename;

        String[] metadata = storageService.getMetadataFromBlob(bucketName, this.filename);

        Identifier identifier = Identifier.newBuilder().setIdentification(idBlob).build();

        // send the result to the client
        identificationStream.onNext(identifier);
        identificationStream.onCompleted();

        for(int i = 0; i < 3; i++) {
            // publish the message on pubsub
            boolean isPublished = pubsub.publishMessage(bucketName, this.filename, metadata);
            if(isPublished) break;
        }
    }
}
