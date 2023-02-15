package server;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.firestore.*;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import contractservice.Content;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class FirestoreService {

    private final String COLLECTION_NAME = "translations";
    private final String FIELD_NAME_LABELS = "labels";
    private final String FIELD_NAME_TRANSLATIONS = "translations";
    private final int BEFORE = 0;
    private final int AFTER = 1;
    private final int EQUAL = 2;
    private Storage storage;

    private Firestore db;

    public FirestoreService(Storage storage) {
        this.storage = storage;
    }

    //create the connection to the Firestore
    public void init(String pathFileKeyJson) throws IOException {
        GoogleCredentials credentials = null;
        if (pathFileKeyJson != null) {
            InputStream serviceAccount = new FileInputStream(pathFileKeyJson);
            credentials = GoogleCredentials.fromStream(serviceAccount);
        } else {
            // use GOOGLE_APPLICATION_CREDENTIALS environment variable
            credentials = GoogleCredentials.getApplicationDefault();
        }
        FirestoreOptions options = FirestoreOptions
                .newBuilder().setCredentials(credentials).build();
        db = options.getService();
    }

    public ArrayList<String> getLabels(String identifier) {
        CollectionReference cRef = db.collection(COLLECTION_NAME);
        DocumentReference docRef = cRef.document(identifier);

        ApiFuture<DocumentSnapshot> apiFut = docRef.get();//

        try {
            DocumentSnapshot docSnapShot = apiFut.get();//
            return (ArrayList<String>) docSnapShot.get(FIELD_NAME_LABELS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArrayList<String> getTranslations(String identifier) {
        CollectionReference cRef = db.collection(COLLECTION_NAME);
        DocumentReference docRef = cRef.document(identifier);

        ApiFuture<DocumentSnapshot> apiFut = docRef.get();//

        try {
            DocumentSnapshot docSnapShot = apiFut.get();//
            return (ArrayList<String>) docSnapShot.get(FIELD_NAME_TRANSLATIONS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArrayList<String> getCharacteristicWithDates(String firstDate, String secondDate, String characteristic) {
        ArrayList<String> validIdentifier = new ArrayList<String>();
        ArrayList<String> validDateBlobs = getValidDateFromBlob(firstDate, secondDate);

        for (int i = 0; i < validDateBlobs.size(); i++) {
            ArrayList<String> labelsPortuguese = getLabels(validDateBlobs.get(i));
            if (labelsPortuguese != null) {
                if (labelsPortuguese.contains(characteristic))
                    validIdentifier.add(validDateBlobs.get(i));
            }
        }

        return validIdentifier;
    }

    private ArrayList<String> getValidDateFromBlob(String beginningDate, String endingDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
        ArrayList<String> identifiers = new ArrayList<String>();

        try {
            Date start = sdf.parse(beginningDate);
            Date end = sdf.parse(endingDate);

            for (Bucket bucket : storage.list().iterateAll()) {
                for (Blob blob : bucket.list().iterateAll()) {
                    Date currentDate = new Date(blob.getCreateTime());

                    int comparison_start = compareDates(currentDate, start);
                    int comparison_end = compareDates(currentDate, end);
                    boolean isAfter = (comparison_start == AFTER) && (comparison_end == BEFORE || comparison_end == EQUAL);
                    boolean isEqual = comparison_start == EQUAL && comparison_end == BEFORE;
                    if (isAfter || isEqual)
                        identifiers.add(bucket.getName() + "_" + blob.getName());
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return identifiers;

    }

    // 0 - before, 1 - after, 2 - equal
    private int compareDates(Date date1, Date date2) {
        if (date1.after(date2))
            return AFTER;

        else if (date1.before(date2))
            return BEFORE;

        else
            return EQUAL;
    }

    public ArrayList<String> getAllLabels() {
        ArrayList<String> validIdentifier = new ArrayList<String>();

        for (Bucket bucket : storage.list().iterateAll()) {
            for (Blob blob : bucket.list().iterateAll()) {
                ArrayList<String> labelsTrans = getLabels(bucket.getName() + "_" + blob.getName());
                if (labelsTrans != null) {
                    for (String stLabel : labelsTrans) {
                        if (!validIdentifier.contains(stLabel))
                            validIdentifier.add(stLabel);
                    }
                }
            }
        }
        return validIdentifier;
    }

    public boolean checkExistingDocumentOnFirestore(String bucket, String blobName) {
        CollectionReference cRef = db.collection(COLLECTION_NAME);
        DocumentReference docRef = cRef.document(bucket+"_"+blobName);

        ApiFuture<DocumentSnapshot> apiFut = docRef.get();//

        try {
            DocumentSnapshot docSnapShot = apiFut.get();
            return docSnapShot.exists();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }
}
