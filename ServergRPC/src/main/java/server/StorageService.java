package server;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class StorageService {

    private static String NOT_VALID_BUCKET = "gcf-sources-929277628179-us-central1";
    public static int INDEX_CONTENT_TYPE = 0;
    public static int INDEX_SIZE = 1;
    public static int INDEX_TIME_CREATED = 2;
    private Storage storage;
    private BlobId blobId;
    private BlobInfo blobInfo;
    private WriteChannel writer;

    private String bucketName;
    private String keyBlob;

    // creates the BlobInfo and BlobId to start to write the content on storage
    public StorageService(Storage storage, String keyBlob, String contentType) {
        this.storage = storage;

        Random rd = new Random();
        ArrayList<String> allBuckets = getListBuckets();

        while (true) {
            // sets the content on a random blob
            this.bucketName = allBuckets.get(rd.nextInt(allBuckets.size()));
            if(!this.bucketName.equals(NOT_VALID_BUCKET)) break;
        }

        // creates a name valid name for the blob if the name already exists
        int count = 0;
        while(true) {
            if(!checkExistingBlobName(this.bucketName, keyBlob)) {
                this.keyBlob = keyBlob;
                break;
            }
            else {
                String newKeyBlob = keyBlob+count;
                if(!checkExistingBlobName(this.bucketName, newKeyBlob)) {
                    this.keyBlob = newKeyBlob;
                    break;
                }
                count++;
            }
        }

        this.blobId = BlobId.of(this.bucketName, this.keyBlob);
        this.blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();
        this.writer = storage.writer(blobInfo);
    }

    // uploads the content of the blob on the storage
    public void uploadContentToBlob(ByteString content)  {
        byte[] buffer = content.toByteArray();
        try {
            writer.write(ByteBuffer.wrap(buffer));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // returns all the buckets names
    private ArrayList<String> getListBuckets() {
        ArrayList<String> buckets = new ArrayList<>();

        for (Bucket bucket : storage.list().iterateAll()) {
            buckets.add(bucket.getName());
        }
        return buckets;
    }

    // closes the upload, that way the file will be on storage with success
    public void closeUploadContent() {
        try {
            writer.close();
            makeObjectPublic(this.bucketName, this.keyBlob);
            System.out.println("Blob access URL: " + "https://storage.googleapis.com/" + this.bucketName + "/" + this.keyBlob);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // makes the blob with visibility public
    private void makeObjectPublic(String bucketName, String keyBlob) {
        BlobId blobId = BlobId.of(bucketName, keyBlob);
        storage.createAcl(blobId, Acl.of(Acl.User.ofAllUsers(), Acl.Role.READER));
    }

    // returns true if already exists the blob
    public boolean checkExistingBlobName(String bucketName, String filename) {
        Bucket bucket = storage.get(bucketName);

        for (Blob blob : bucket.list().iterateAll()) {
            if (filename.equals(blob.getName())) return true;
        }
        return false;
    }

    public String[] getMetadataFromBlob(String bucketName, String keyBlob) {
        String[] metadata = new String[3];
        Blob blob = storage.get(bucketName, keyBlob);

        String contentType = blob.getContentType();
        String size = ""+blob.getSize();
        String timeCreated = ""+new Date(blob.getCreateTime());

        metadata[INDEX_CONTENT_TYPE] = contentType;
        metadata[INDEX_SIZE] = size;
        metadata[INDEX_TIME_CREATED] = timeCreated;
        return metadata;
    }

    public String getBucketName() {
        return this.bucketName;
    }

    public String getFilename() {
        return this.keyBlob;
    }
}
