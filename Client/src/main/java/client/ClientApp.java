package client;


import com.google.protobuf.ByteString;
import contractservice.*;
import contractservice.Void;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ClientApp {

    private static final int MAX_RECALL = 4;
    private static final int svcPort = 8000;
    private static final String INSTANCE_NAME = "instance-template-server";

    private static ManagedChannel channel;
    private static ContractServiceGrpc.ContractServiceBlockingStub blockingStub;
    private static ContractServiceGrpc.ContractServiceStub noBlockStub;

    private static Menu menu;
    private static String instanceGroupName;

    public static void main(String[] args) {
        try {
            boolean connectionServer = getConnection();

            if (connectionServer)
                chooseInformation();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static String getIpAddresses(String instanceGroup) {
        String cfURL = "https://us-central1-serious-fabric-252921.cloudfunctions.net/funcHttpIP?server=" + instanceGroup;
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(cfURL)).GET().build();


        HttpResponse<String> response = null;
        try {
            String[] result = null;
            response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                result = response.body().split(";");
                Menu.printIpAddresses(result);
                return randomIpChooser(result);
            } else
                System.out.println("Error receiving Ip addresses!");

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static boolean createConnectionWithServer(String instanceName) {
        // choose a random Ip address
        String ipAddress = getIpAddresses(instanceName);

        if (ipAddress != null) {
            System.out.println("Ip Address chosen: " + ipAddress);
            channel = ManagedChannelBuilder.forAddress(ipAddress, svcPort).usePlaintext().build();

            blockingStub = ContractServiceGrpc.newBlockingStub(channel);
            noBlockStub = ContractServiceGrpc.newStub(channel);

            // sends a ping to the server, if true means there is connection
            boolean haveConnection = checkConnectionWithServer();
            return haveConnection;
        } else {
            System.out.println("The instance group name don't have any server on!");
            return false;
        }
    }

    public static boolean getConnection() {
        boolean hasConnection = true;
        while (true) {
            String instanceName = Menu.chooseInstanceName(INSTANCE_NAME);
            instanceGroupName = instanceName;
            if (!instanceName.equals("None")) {
                boolean haveConnection = createConnectionWithServer(instanceName);

                if (haveConnection) {
                    menu = new Menu();
                    System.out.println("Success connecting to the server!\n");
                    break;
                } else
                    System.out.println("There is no connection with the Instance Group Name! Retry another...\n");

            } else {
                hasConnection = false;
                break;
            }
        }
        return hasConnection;
    }

    //sends the file to the server on chunks
    public static boolean sendFileBlocks(String[] information) throws IOException {
        Path uploadFrom = Paths.get(information[0]);
        System.out.println("Path: " + uploadFrom.toString());
        String contentType = Files.probeContentType(uploadFrom);

        ClientStream rpyStreamObs = new ClientStream();
        StreamObserver<Content> reqs = noBlockStub.sendFileBlocks(rpyStreamObs);

        boolean firstTime = true;
        byte[] buffer = new byte[1024];
        try (InputStream input = Files.newInputStream(uploadFrom)) {
            int limit;
            while ((limit = input.read(buffer)) >= 0) {
                try {
                    byte[] contentsRaw = Arrays.copyOfRange(buffer, 0, limit);
                    ByteString contentByte = ByteString.copyFrom(contentsRaw);
                    Content.Builder contents = Content.newBuilder().setFileBlockBytes(contentByte);
                    if (firstTime) {
                        firstTime = false;
                        contents.setFilename(information[1]).setContentType(contentType);
                    }
                    Content content = contents.build();
                    reqs.onNext(content);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            reqs.onCompleted();
            // to assure that the content is received
            while (!rpyStreamObs.isComplete()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            String ident = rpyStreamObs.getIdentification();
            if (ident != null) {
                System.out.println("Stream complete! Id received: " + ident + "\n");
                return true;
            }
            return false;
        }

    }

    public static void recallSendFileBlocks(String[] information, int timeRecalled) throws IOException {
        boolean received = sendFileBlocks(information);

        if (!received) {
            System.out.println("Error sending the image! Retrying another server...");
            if (timeRecalled < MAX_RECALL) {
                createConnectionWithServer(instanceGroupName);
                recallSendFileBlocks(information, timeRecalled++);
            } else
                throw new IllegalCallerException("Too much retrying another server!");
        }
    }

    // check if there is connection with server with a certain IP
    public static boolean checkConnectionWithServer() {
        try {
            Void request = Void.newBuilder().build();
            Void vd = blockingStub.checkServerConnection(request);
            return vd != null;
        } catch (Exception e) {
            System.out.println("Error trying to connect to the server! Retrying...");
            return false;
        }
    }

    // gets all the images labels existing
    public static ArrayList<String> getAllCharacteristics() {
        Void vd = Void.newBuilder().build();
        PortugueseLabels labels = blockingStub.getAllLabelNames(vd);

        var labelsStr = labels.getLabelsList();
        ArrayList<String> labelNames = new ArrayList<String>();
        for (int i = 0; i < labelsStr.size(); i++) {
            labelNames.add(labelsStr.get(i));
        }
        return labelNames;
    }

    public static ArrayList<String> recallGetAllCharacteristics(int timeRecalled) {
        try {
            return getAllCharacteristics();
        } catch (Exception e) {
            System.out.println("Error getting the characteristics! Retrying another server...");
            if (timeRecalled < MAX_RECALL) {
                createConnectionWithServer(instanceGroupName);
                return recallGetAllCharacteristics(timeRecalled++);
            } else
                throw new IllegalCallerException("Too much retrying another server!");
        }
    }

    // synchonized call
    public static HashMap<String, ArrayList<String>> getAllFilesFromBucket() {
        Void vd = Void.newBuilder().build();

        ArrayList<String> blobNames = new ArrayList<>();
        HashMap<String, ArrayList<String>> mapKeys = new HashMap<String, ArrayList<String>>();

        Iterator<Name> identReceived = blockingStub.getAllFilesFromStorage(vd);

        while (identReceived.hasNext()) {
            Name nameIdent = identReceived.next();
            String bucketName = nameIdent.getBucketName();
            String blobName = nameIdent.getFilename();
            ArrayList<String> blobsForBucket = mapKeys.get(bucketName) == null ? new ArrayList<String>() : mapKeys.get(bucketName);
            blobsForBucket.add(blobName);
            mapKeys.put(bucketName, blobsForBucket);
        }

        return mapKeys;
    }

    public static HashMap<String, ArrayList<String>> recallGetAllFilesFromBucket(int timeRecalled) {
        try {
            return getAllFilesFromBucket();
        } catch (Exception e) {
            System.out.println("Error getting the characteristics! Retrying another server...");
            if (timeRecalled < MAX_RECALL) {
                createConnectionWithServer(instanceGroupName);
                return recallGetAllFilesFromBucket(timeRecalled++);
            } else
                throw new IllegalCallerException("Too much retrying another server!");
        }
    }

    // reveive content and write the bytes on the disk
    public static void downloadBlob(String bucketName, String blobName, String pathWrite) throws IOException {
        Name name = Name.newBuilder().setBucketName(bucketName).setFilename(blobName).build();

        ArrayList<ByteString> blobNames = new ArrayList<>();

        Iterator<Content> contentReceived = blockingStub.getContentStored(name);

        String type = null;

        int size = 0;
        // adds the content to a arraylist
        while (contentReceived.hasNext()) {
            Content content = contentReceived.next();
            // to get the type of the image
            if (size == 0) {
                type = content.getContentType().split("/")[1];
            }
            ByteString bs = content.getFileBlockBytes();
            blobNames.add(bs);
            byte[] bytes = bs.toByteArray();
            size += bytes.length;
        }

        byte[] imageBytes = new byte[size];

        // writtes all the content on one byte array
        int chunk = 0;
        for (int i = 0; i < blobNames.size(); i++) {
            byte[] bytes = blobNames.get(i).toByteArray();
            for (int j = 0; j < bytes.length; j++) {
                imageBytes[chunk] = bytes[j];
                chunk++;
            }

        }
        ByteArrayInputStream bis = new ByteArrayInputStream(imageBytes);
        BufferedImage bImage2 = ImageIO.read(bis);
        String pathname = pathWrite + "." + type;
        ImageIO.write(bImage2, type, new File(pathname));
        System.out.println("Success receiving content! Path: " + pathname + "\n");

    }

    public static void recallDownloadBlob(String bucketName, String blobName, String pathWrite, int timeRecalled) {
        try {
            downloadBlob(bucketName, blobName, pathWrite);
        } catch (Exception e) {
            System.out.println("Error downloading the image! Retrying another server...");
            if (timeRecalled < MAX_RECALL) {
                createConnectionWithServer(instanceGroupName);
                recallDownloadBlob(bucketName, blobName, pathWrite, timeRecalled++);
            } else
                throw new IllegalCallerException("Too much retrying another server!");
        }
    }

    public static void getCharacteristicsAndTranslation(String bucket, String filename) {
        String idenStr = bucket + "_" + filename;
        Identifier identifier = Identifier.newBuilder().setIdentification(idenStr).build();

        // gets a string with all the labels
        PortugueseLabels labels = blockingStub.getCharacteristics(identifier);
        var labelsList = labels.getLabelsList();
        String labelString = "Labels found:       ";
        for (int i = 0; i < labelsList.size(); i++) {
            labelString += labelsList.get(i) + " | ";
        }
        System.out.println();
        System.out.println(labelString);

        // gets all the translations
        var trans = labels.getTranslationsList();
        String translationsString = "Translations found: ";
        for (int i = 0; i < trans.size(); i++) {
            translationsString += trans.get(i) + " | ";
        }
        System.out.println(translationsString + "\n");
    }

    public static void recallGetCharacteristicsAndTranslation(String bucketName, String blobName, int timeRecalled) {
        try {
            getCharacteristicsAndTranslation(bucketName, blobName);
        } catch (Exception e) {
            System.out.println("Error getting translations and labels! Retrying another server...");
            if (timeRecalled < MAX_RECALL) {
                createConnectionWithServer(instanceGroupName);
                recallGetCharacteristicsAndTranslation(bucketName, blobName, timeRecalled++);
            } else
                throw new IllegalCallerException("Too much retrying another server!");
        }
    }

    public static String[] getFileStoredWithDateAndCharacteristic() {
        String[] result = new String[3];

        ArrayList<String> labelNames = recallGetAllCharacteristics(0);

        String date1 = menu.selectDate("Select the beginning date!");
        String date2 = menu.selectDate("Select the ending date!");

        int resultCharacterist = menu.selectCharacteristic(labelNames);
        if (resultCharacterist != 0) {
            String labelName = labelNames.get(resultCharacterist - 1);

            result[0] = date1;
            result[1] = date2;
            result[2] = labelName;

            return result;
        } else return null;
    }

    public static void getIdentifierWithDateCharacteristic(String[] values) {
        Characteristics crc = Characteristics.newBuilder().setFirstDate(values[0]).
                setSecondDate(values[1]).setCharacteristicsInformation(values[2]).build();

        var labels = blockingStub.getFilesWithCharacteristics(crc);

        String result = "Identifications: ";
        while (labels.hasNext()) {
            Identifier inf = labels.next();
            result += inf.getIdentification() + " | ";
        }
        System.out.println(result + "\n");
    }

    public static void recallGetIdentifierWithDateCharacteristic(String[] values, int timeRecalled) {
        try {
            getIdentifierWithDateCharacteristic(values);
        } catch (Exception e) {
            System.out.println("Error getting the identifiers of the images! Retrying another server...");
            if (timeRecalled < MAX_RECALL) {
                createConnectionWithServer(instanceGroupName);
                recallGetIdentifierWithDateCharacteristic(values, timeRecalled++);
            } else
                throw new IllegalCallerException("Too much retrying another server!");
        }
    }

    public static void chooseInformation() throws IOException {
        int selectAction = 1;

        while (selectAction > 0) {
            selectAction = menu.chooseAllOrDateCharacteristic();
            if (selectAction == 1) {
                int info = menu.chooseListOrSendBlob();

                switch (info) {
                    // send image to server
                    case 1:
                        String[] filePath = menu.sendFileToStorage();
                        recallSendFileBlocks(filePath, 0);
                        break;
                    case 2:
                        HashMap<String, ArrayList<String>> blobs = recallGetAllFilesFromBucket(0);
                        menu.setBlobNames(blobs);
                        selectAction = menu.selectBlobFromStorage();

                        if (selectAction != 0) {
                            // gets the bucket and blob name
                            String[] bucketAndBlobSelected = menu.getNameAndBucketByNumber(selectAction);

                            int selection = menu.selectOperationOnBlob();

                            if (selection == 1) {
                                String pathWrite = menu.getPathToWrite();
                                String filename = menu.getContentName();
                                recallDownloadBlob(bucketAndBlobSelected[0], bucketAndBlobSelected[1], pathWrite + "/" + filename, 0);
                            } else
                                recallGetCharacteristicsAndTranslation(bucketAndBlobSelected[0], bucketAndBlobSelected[1], 0);
                        }
                        break;

                    default:
                        selectAction = 0;
                        break;
                }
            } else if (selectAction == 2) {
                String[] info = getFileStoredWithDateAndCharacteristic();
                if (info != null)
                    recallGetIdentifierWithDateCharacteristic(info, 0);
                    //getIdentifierWithDateCharacteristic(info);
                else
                    selectAction = 0;
            }
        }

    }

    private static String randomIpChooser(String[] ipAddresses) {
        Random rd = new Random();
        if (ipAddresses.length == 0)
            return null;
        return ipAddresses[rd.nextInt(ipAddresses.length)];
    }

}
