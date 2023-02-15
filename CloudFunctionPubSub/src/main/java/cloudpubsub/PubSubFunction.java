package cloudpubsub;


import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;
import io.grpc.netty.shaded.io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class PubSubFunction implements BackgroundFunction<Message> {

    private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());
    private static final Firestore firestore = getFirestore();
    private static final String KEY_TRANSLATIONS = "translations";
    private static final String COLLECTION_NAME = "translations";
    private static final String LABELS_NAME = "labels";
    private static final String[] KEYS = new String[]{"messageId", "type", "size", "time", "processedTime"};

    // when it's instanced it gets the firestore automatically
    private static Firestore getFirestore() {
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            FirestoreOptions options = FirestoreOptions.newBuilder().setCredentials(credentials).build();
            return options.getService();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void accept(Message message, Context context) {
        if (firestore == null) {
            logger.info("Error connecting to Firestore. Exiting function");
            throw new RuntimeException("Error connecting to firestore");
        }

        String data = new String(Base64.getDecoder().decode(message.data));
        String[] bucketFilename = data.split(";");
        //logger.info("decode: "+data);

        HashMap<String, String> attributes = message.attributes;

        // remove the keys that constains aditional information to get a map with only labels
        String[] info = new String[]{attributes.get(KEYS[0]), attributes.get(KEYS[1]), attributes.get(KEYS[2]), attributes.get(KEYS[3]), attributes.get(KEYS[4])};
        for (int i = 0; i < 5; i++) {
            attributes.remove(KEYS[i]);
        }

        // sets the labels to arraylist
        ArrayList<String> labels = new ArrayList<String>();
        for (Object value : attributes.values()) {
            labels.add((String) value);
        }

        // gets translations
        ArrayList<String> translations = getTranslations(labels);

        if (translations == null) {
            logger.info("Error connecting obtaining the Labels.");
            throw new RuntimeException("Error obtaining the labels");
        }

        HashMap<String, Object> translationMap = new HashMap<String, Object>() {
            {
                put(KEYS[0], info[0]);
                put(KEYS[1], info[1]);
                put(KEYS[2], info[2]);
                put(KEYS[3], info[3]);
                put(KEYS[4], info[4]);
                put(LABELS_NAME, labels);
                put(KEY_TRANSLATIONS, translations);
            }
        };
        //sends info to firestore
        setInfoToFirestore(bucketFilename, translationMap);

    }

    private ArrayList<String> getTranslations(ArrayList<String> labels) {
        ArrayList<String> translations = null;

        try {
            translations = new ArrayList<String>();
            Translate translate = TranslateOptions.getDefaultInstance().getService();

            var english = Translate.TranslateOption.sourceLanguage("en");
            var portuguese = Translate.TranslateOption.targetLanguage("pt");

            for (int i = 0; i < labels.size(); i++) {
                Translation translation = translate.translate(labels.get(i), english, portuguese);
                translations.add(translation.getTranslatedText());
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Error obtaining the Translations!");
        }
        return translations;
    }

    private void setInfoToFirestore(String[] bucketFilename, HashMap<String, Object> translationMap) {
        CollectionReference colRef = firestore.collection(COLLECTION_NAME);

        //adds the document to the colletion
        String docName = bucketFilename[0] + "_" + bucketFilename[1];
        DocumentReference docRef = colRef.document(docName);

        ApiFuture<WriteResult> result = docRef.set(translationMap); //Overwrites the document


        while (!result.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            // Has to have the get to set the result correctly
            var writeResult = result.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            logger.info("Error writing to Firestore!");
        }
    }
}
