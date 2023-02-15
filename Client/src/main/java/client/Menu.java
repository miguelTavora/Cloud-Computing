package client;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Menu {

    private HashMap<String, ArrayList<String>> blobNames;
    private Scanner sc;
    private ArrayList<String> orderedMap = new ArrayList<String>();

    public Menu() {
        sc = new Scanner(System.in);
    }

    public String retryAnotherName() {
        System.out.println("The name already exists!");
        return getContentName();
    }

    public void setBlobNames(HashMap<String, ArrayList<String>> blobNames) {
        this.blobNames = blobNames;
    }


    public static String chooseInstanceName(String defaultName) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Choose the instance-group name!");
        System.out.println("Press: \"Enter\" to select default instance ("+defaultName+")");
        System.out.println("Write: \"None\" to exit");

        String userInput = sc.nextLine();
        if (userInput.equals("")) return defaultName;

        return userInput;
    }

    public int chooseAllOrDateCharacteristic() {
        System.out.println("Choose one of the following numbers to select the bucket:");
        System.out.println("0 - Exit");
        System.out.println("1 - Operations with Images");
        System.out.println("2 - List Images with date and one characteristic");

        while (true) {
            String userInput = sc.nextLine();
            // regular expression if not select a number
            if (userInput.matches("[^0-9]*")) System.out.println("Not a number! Try again:");

            else {
                int number = Integer.parseInt(userInput);
                if (number == 0) break;

                else if (number == 1 || number == 2) {
                    return number;

                } else System.out.println("Not a valid number! Try again");
            }
        }
        return 0;
    }

    public String selectDate(String print) {
        System.out.println(print);
        System.out.println("The correct format is: dd-MM-yyyy");

        while (true) {
            String userInput = sc.nextLine();

            if (!isDateValid(userInput))
                System.out.println("Not a valid date! Try again:");

            else
                return userInput;
        }
    }

    public int selectCharacteristic(ArrayList<String> names) {
        System.out.println("Select what you want!");
        System.out.println("0 - Exit");
        printStringList(names);

        while (true) {
            String userInput = sc.nextLine();
            if (userInput.matches("[^0-9]*")) System.out.println("Not a number! Try again:");

            else {
                int number = Integer.parseInt(userInput);

                if (number == 0) break;

                if (number > 0 && number < names.size() + 1)
                    return number;

                else System.out.println("Not a valid number! Try again:");
            }

        }
        return 0;
    }

    private boolean isDateValid(String date) {
        try {
            DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
            df.setLenient(false);
            df.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public int chooseListOrSendBlob() {
        System.out.println("Select what you want!");
        System.out.println("1 - Send Image to be classified");
        System.out.println("2 - Select one Image from the Server");


        while (true) {
            String userInput = sc.nextLine();
            // regular expression if not select a number
            if (userInput.matches("[^0-9]*")) System.out.println("Not a number! Try again:");
            else {
                int number = Integer.parseInt(userInput);

                if (number > 0 && number < 3)
                    return number;

                else System.out.println("Not a valid number! Try again:");
            }

        }

    }

    public String[] sendFileToStorage() {
        // calls the path and name of the file
        String pathFile = choosePathFile();
        String contentName = getContentName();
        return new String[]{pathFile, contentName};
    }

    public int selectBlobFromStorage() {
        System.out.println("Select one of the Image names:");
        System.out.println("0 - Exit");
        int size = printMapWithContent();

        while (true) {
            String userInput = sc.nextLine();

            if (userInput.matches("[^0-9]*")) System.out.println("Not a number! Try again:");

            else {
                int number = Integer.parseInt(userInput);
                if (number == 0) break;

                else if (number > 0 && number < size)
                    return number;

                else System.out.println("Not a valid number! Try again");
            }

        }
        return 0;

    }

    public String[] getNameAndBucketByNumber(int number) {
        int count = 0;
        for(int i = 0; i < orderedMap.size(); i++) {
            ArrayList<String> blobs = blobNames.get(orderedMap.get(i));
            if(count+blobs.size() >= number) {
                return new String[] {orderedMap.get(i), blobs.get(number-count-1)};
            }
            count += blobs.size();
        }
        return null;
    }

    public int selectOperationOnBlob() {
        System.out.println("Select what you want!");
        System.out.println("1 - Download blob from Storage");
        System.out.println("2 - See characteristics and is translation");

        while (true) {
            String userInput = sc.nextLine();

            if (userInput.matches("[^0-9]*")) System.out.println("Not a number! Try again:");

            else {
                int number = Integer.parseInt(userInput);

                if (number == 1 || number == 2) return number;

                else System.out.println("Not a valid number! Try again");
            }

        }
    }

    //select the path to write the file on the client machine
    public String getPathToWrite() {
        System.out.println("Write here the path to the file:");
        while (true) {
            String path = sc.nextLine();
            path = path.replace("\\", "/");

            File f = new File(path);
            if (f.isDirectory()) return path;

            else System.out.println("The directory don't exists! Try again:");

        }
    }

    // choose the path to the file
    private String choosePathFile() {
        System.out.println("Write here the path to the file:");

        while (true) {
            String path = sc.nextLine();
            path = path.replace("\\", "/");

            File f = new File(path);
            if (f.exists() && !f.isDirectory()) return path;

            else System.out.println("The file don't exists! Try again:");

        }
    }

    // choose the name of the content on the database
    public String getContentName() {
        System.out.println("Choose the name to the content:");

        while (true) {
            String name = sc.nextLine();

            if (name != null && name.length() == 0)
                System.out.println("The name is not valid! Try again:");

            else if (!name.matches("[A-Za-z0-9_]+"))
                System.out.println("The name can only contain numbers, letters and _! Try again:");

            else return name;

        }
    }

    public static void printIpAddresses(String[] ipAddresses) {
        String result = "Received Ip Addresses: ";
        for (int i = 0; i < ipAddresses.length; i++) {
            result += ipAddresses[i]+" | ";
        }
        System.out.println(result);
    }

    private void printStringList(ArrayList<String> names) {
        for (int i = 0; i < names.size(); i++) {
            System.out.println((i + 1) + " - " + names.get(i));
        }
    }

    private int printMapWithContent() {
        orderedMap = new ArrayList<String>();
        int count = 1;
        for (String bucketName : blobNames.keySet()) {
            ArrayList<String> blobs = blobNames.get(bucketName);
            for(int i = 0; i < blobs.size();i++) {
                System.out.println(""+count+" - "+blobs.get(i));
                count++;
            }
            orderedMap.add(bucketName);
        }
        return count;
    }
}
