import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class pc extends Thread {

    private HashMap<Integer, String> networkMap=null;
    private ArrayList<String> replicationNodes= null;

    private int portAddress=0;
    private String localAddress=null;
    private String filesLocation=null;

    public pc(){
        networkMap=FileTS.getNetWM();
        replicationNodes=FileTS.getRNodes();
        portAddress=FileTS.getpSPort();
        localAddress=FileTS.getLocAddr();
        filesLocation=FileTS.getFileloc();
    }

    //thread implementation for peer to serve as client

    public void run() {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in))) {
            HashMap<String, String> hm = retrieveHashTable();
            if (hm != null) {
                for (Map.Entry e : hm.entrySet()) {
                    FileTS.putinHT(e.getKey().toString(), e.getValue().toString(), true);
                }
            }
            
            if (replicationNodes.contains(localAddress)) {
                System.out.println("****** REPLICATION SERVICE STARTED ******");
                replication service = new replication(null, null, "REPLICATE");
                service.start();
            }
            
            while (true) {
                System.out.println("\nWhat do you want to do?");
                System.out.println("1.Register a file with the peers.");
                System.out.println("2.Search for a file.");
                System.out.println("3.Un-register a file with the peers.");
                System.out.println("4.Print log of this peer.");
                System.out.println("5.Exit.");
                System.out.print("Enter choice and press ENTER:");
                
                int option;
                try {
                    option = Integer.parseInt(input.readLine());
                } catch (NumberFormatException e) {
                    System.out.println("Wrong choice. Try again!!!");
                    continue;
                }
                
                switch (option) {
                    case 1:
                        registerFile(input);
                        break;
                    case 2:
                        searchForFile(input);
                        break;
                    case 3:
                        unregisterFile(input);
                        break;
                    case 4:
                        printLog();
                        break;
                    case 5:
                        exitPeer(input);
                        break;
                    default:
                        System.out.println("Wrong choice. Try again!!!");
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //5 different functions for all the above cases
    //register file
    private void registerFile(BufferedReader input) throws IOException {
        System.out.println("\nEnter name of the file with extension (Example: data.txt) you want to put in the file sharing system:");
        String fileName = input.readLine();
        
        if (fileName.trim().length() == 0) {
            System.out.println("Invalid Filename.");
            return;
        }
        
        File file = new File(filesLocation + fileName);
        
        if (file.exists()) {
            long startTime = System.currentTimeMillis();
            if (put(fileName, localAddress)) {
                System.out.println(fileName + " added to the file sharing system and is available to download for other peers.");
            } else {
                System.out.println("Unable to add " + fileName + " to the file sharing system. Please try again later.");
            }
            double time = (double) Math.round(System.currentTimeMillis() - startTime) / 1000;
            System.out.println("Time Taken: " + time + " seconds.");
        } else {
            System.out.println("File with the given filename does not exist in [" + filesLocation + "] path.");
        }
    }
    
    
    //search for a file
    private void searchForFile(BufferedReader input) throws IOException {
        System.out.println("\nEnter name of the file you want to look for:");
        String fileName = input.readLine();
        String hostAddress;
        
        long startTime = System.currentTimeMillis();
        String value = get(fileName);
        double time = (double) Math.round(System.currentTimeMillis() - startTime) / 1000;
        
        if (value != null) {
            System.out.println("File Found. Lookup time: " + time + " seconds.");
            hostAddress = value;
            System.out.println("The file will be downloaded in the 'downloads' folder in the current location.");
            obtain(hostAddress, portAddress, fileName);
    
            // if (fileName.trim().endsWith(".txt")) {
            //     System.out.print("\nDo you want to download (D) or print this file (P)? Enter (D/P):");
            //     String download = input.readLine();
            //     if (download.equalsIgnoreCase("D")) {
            //         System.out.println("The file will be downloaded in the 'downloads' folder in the current location.");
            //         obtain(hostAddress, portAddress, fileName);
            //     } else if (download.equalsIgnoreCase("P")) {
            //         obtain(hostAddress, portAddress, fileName);
            //         FileUT.printFile(fileName);
            //     }
            // } else {
            //     System.out.print("\nDo you want to download this file? (Y/N):");
            //     String download = input.readLine();
            //     if (download.equalsIgnoreCase("Y")) {
            //         obtain(hostAddress, portAddress, fileName);
            //     }
            // }
        } else {
            System.out.println("File not found. Lookup time: " + time + " seconds.");
        }
    }
    
    
    //unregister a file
    private void unregisterFile(BufferedReader input) throws IOException {
        System.out.println("\nEnter the name of the file you want to remove from the file sharing system:");
        String key = input.readLine();
        
        if (!validateKey(key)) {
            return;
        }
        
        System.out.print("\nAre you sure (Y/N)?:");
        String confirm = input.readLine();
        
        if (confirm.equalsIgnoreCase("Y")) {
            long startTime = System.currentTimeMillis();
            if (delete(key)) {
                System.out.println("The file was successfully removed from the file sharing system.");
            } else {
                System.out.println("There was an error in removing the file. Please try again later.");
            }
            double time = (double) Math.round(System.currentTimeMillis() - startTime) / 1000;
            System.out.println("Time taken: " + time + " seconds");
        }
    }
    
    
    //print log
    private void printLog() {
        (new LogUT("peer")).print();
    }
    
    
    //exit peer
    private void exitPeer(BufferedReader input) throws IOException {
        System.out.print("\nThe files shared by this peer will no longer be accessible by other peers in this network. Are you sure you want to exit? (Y/N)?:");
        String confirm = input.readLine();
        
        if (confirm.equalsIgnoreCase("Y")) {
            System.out.println("Thanks for using this system.");
            System.exit(0);
        }


    }


    //download file
    private void obtain(String hostAddress, int port, String fileName) {
        long startTime = System.currentTimeMillis();
        boolean isDownloaded = false;
    
        if (FileUT.downloadFile(hostAddress, port, fileName, false)) {
            isDownloaded = true;
        } else {
            List<String> backupNodes = FileTS.getRNodes();
    
            for (String node : backupNodes) {
                if (FileUT.downloadFile(node, port, fileName, true)) {
                    isDownloaded = true;
                    break;
                }
            }
        }
    
        long endTime = System.currentTimeMillis();
        double time = (double) Math.round(endTime - startTime) / 1000;
    
        if (isDownloaded) {
            System.out.println("File downloaded successfully in " + time + " seconds.");
        } else {
            System.out.println("Unable to connect to the host. Unable to download the file. Try using a different peer if available.");
        }
    }



//retrives hashtable from one of the replication nodes
    private HashMap<String, String> retrieveHashTable() {
    HashMap<String, String> hm = null;

    for (String nodeAddress : replicationNodes) {
        if (nodeAddress.equalsIgnoreCase(localAddress)) {
            continue;
        }

        try (Socket socket = new Socket(nodeAddress, portAddress);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            REQ peerRequest = new REQ();
            peerRequest.setRequestType("GET_R_HASHTABLE");
            out.writeObject(peerRequest);

            RES serverResponse = (RES) in.readObject();
            if (serverResponse != null && serverResponse.getResponseCode() == 200) {
                hm = (HashMap<String, String>) serverResponse.getResponseData();
                break;
            }
        } catch (Exception ex) {
            ex.printStackTrace(); // Handle the exception appropriately.
        }
    }

    // System.out.println("hm = " + hm);
    return hm;
}


    //this method is used to register a file on a network
    private boolean put(String key, String value) {
        try {
            int node = hash(key);
            String nodeAddress = networkMap.get(node);
    
            if (nodeAddress.equals(localAddress)) {
                if (FileTS.getHT().containsKey(key)) {
                    System.out.print("\nA file with the same name is already registered by this peer. Would you like to overwrite it? (Y/N): ");
                    String confirm = (new BufferedReader(new InputStreamReader(System.in))).readLine();
    
                    if (confirm.equalsIgnoreCase("Y")) {
                        FileTS.putinHT(key, value, true);
                        new replication(key, value, "REGISTER").start();
                        return true;
                    }
                } else {
                    FileTS.putinHT(key, value, true);
                    new replication(key, value, "REGISTER").start();
                    return true;
                }
            } else {
                try (Socket socket = new Socket(nodeAddress, portAddress);
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
    
                    String data = key + "," + value;
                    REQ peerRequest = new REQ();
                    peerRequest.setRequestType("REGISTER");
                    peerRequest.setRequestData(data);
                    out.writeObject(peerRequest);
    
                    RES serverResponse = (RES) in.readObject();
    
                    if (serverResponse.getResponseCode() == 200) {
                        return true;
                    } else if (serverResponse.getResponseCode() == 300) {
                        System.out.print("\nA VALUE with the specified KEY already exists in the Distributed Hash Table. Would you like to overwrite it? (Y/N): ");
                        String confirm = (new BufferedReader(new InputStreamReader(System.in))).readLine();
    
                        if (confirm.equalsIgnoreCase("Y")) {
                            return forcePut(key, value);
                        }
                    } else {
                        // Handle the response appropriately.
                        return false;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    //this method forces a file to register on a network
    private boolean forcePut(String key, String value) {
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        REQ peerRequest = null;
        RES serverResponse = null;
        String data = key + "," + value;
    
        long startTime, endTime;
        double time;
    
        try {
            startTime = System.currentTimeMillis();
    
            int node = hash(key);
            String nodeAddress = networkMap.get(node);
    
            if (nodeAddress.equals(localAddress)) {
                boolean result = FileTS.getHT().containsKey(key);
    
                if (result) {
                    System.out.print("\nA file with the same name is already registered by this peer. Would you like to overwrite it? (Y/N): ");
                    String confirm = (new BufferedReader(new InputStreamReader(System.in))).readLine();
    
                    if (confirm.equalsIgnoreCase("Y")) {
                        FileTS.putinHT(key, value, true);
    
                        replication service = new replication(key, value, "REGISTER");
                        service.start();
    
                        endTime = System.currentTimeMillis();
                        time = (double) Math.round(endTime - startTime) / 1000;
                        System.out.println("Time taken: " + time + " seconds");
    
                        return true;
                    }
                } else {
                    FileTS.putinHT(key, value, true);
    
                    replication service = new replication(key, value, "REGISTER");
                    service.start();
    
                    endTime = System.currentTimeMillis();
                    time = (double) Math.round(endTime - startTime) / 1000;
                    System.out.println("Time taken: " + time + " seconds");
    
                    return true;
                }
            } else {
                socket = new Socket(nodeAddress, portAddress);
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
    
                peerRequest = new REQ();
                peerRequest.setRequestType("REGISTER_FORCE");
                peerRequest.setRequestData(data);
                out.writeObject(peerRequest);
    
                serverResponse = (RES) in.readObject();
    
                if (serverResponse.getResponseCode() == 200) {
                    endTime = System.currentTimeMillis();
                    time = (double) Math.round(endTime - startTime) / 1000;
                    System.out.println("Time taken: " + time + " seconds");
                    return true;
                } else {
                    System.out.print(serverResponse.getResponseData());
                    return false;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null)
                    out.close();
    
                if (in != null)
                    in.close();
    
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    
        return false;
    }


    //searches for a file on a network
    private String get(String key) {
        String value = null;
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        REQ peerRequest = null;
        RES serverResponse = null;
        String nodeAddress=null;
        
        try {
            int node = hash(key);
            nodeAddress = networkMap.get(node);
    
            if (nodeAddress.equals(localAddress)) {
                value = FileTS.getfromHT(key);
            } else {
                socket = new Socket(nodeAddress, portAddress);
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
    
                peerRequest = new REQ();
                peerRequest.setRequestType("LOOKUP");
                peerRequest.setRequestData(key);
                out.writeObject(peerRequest);
    
                serverResponse = (RES) in.readObject();
    
                if (serverResponse.getResponseCode() == 200) {
                    String[] responseData = serverResponse.getResponseData().toString().split(",");
                    if (responseData.length >= 2) {
                        value = responseData[1].trim();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null)
                    out.close();
    
                if (in != null)
                    in.close();
    
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    
        if (value == null) {
            // Handle replication search if the primary node didn't return a result
            value = searchReplica(key, nodeAddress);
        }
    
        return value;
    }


    //this function searches for a file on a replica
    private String searchReplica(String key, String peerAddress) {
        String value = null;
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        REQ peerRequest = null;
        RES serverResponse = null;
    
        for (String nodeAddress : replicationNodes) {
            try {
                if (nodeAddress.equals(localAddress)) {
                    value = FileTS.getfromREP_HT(key);
                } else {
                    socket = new Socket(nodeAddress, portAddress);
                    out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    in = new ObjectInputStream(socket.getInputStream());
    
                    peerRequest = new REQ();
                    peerRequest.setRequestType("R_LOOKUP");
                    peerRequest.setRequestData(key);
                    out.writeObject(peerRequest);
    
                    serverResponse = (RES) in.readObject();
    
                    if (serverResponse.getResponseCode() == 200) {
                        String[] responseData = serverResponse.getResponseData().toString().split(",");
                        if (responseData.length >= 2) {
                            value = responseData[1].trim();
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (out != null)
                        out.close();
    
                    if (in != null)
                        in.close();
    
                    if (socket != null)
                        socket.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
    
            if (value != null) {
                break;
            }
        }
        return value;
    }


    //unregistrs file from a netwrok
    private boolean delete(String key) {
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        REQ peerRequest = null;
        RES serverResponse = null;
        String nodeAddress=null;
        
        int node = hash(key);
        nodeAddress = networkMap.get(node);
    
        try {
            // Make a connection with the server using the specified Host Address and Port portAddress
            socket = new Socket(nodeAddress, portAddress);
            
            // Initialize the output stream using the socket's output stream
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            
            // Initialize the input stream using the socket's input stream
            in = new ObjectInputStream(socket.getInputStream());
    
            // Setup a Request object with Request Type = UNREGISTER and Request Data = KEY
            peerRequest = new REQ();
            peerRequest.setRequestType("UNREGISTER");
            peerRequest.setRequestData(key);
            out.writeObject(peerRequest);
            
            // Read the response message from the server
            serverResponse = (RES) in.readObject();
    
            if (serverResponse.getResponseCode() == 200) {
                return true;
            } else {
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            // Handle exceptions as needed
            e.printStackTrace();
        } finally {
            try {
                // Close all streams if they are initialized
                if (out != null)
                    out.close();
                
                if (in != null)
                    in.close();
                
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        return false;
    }

    private int hash(String key) {
        int hash = 5381; // Initial hash value
    
        for (int i = 0; i < key.length(); i++) {
            hash = (hash * 33) ^ key.charAt(i); // DJB2 hash function
        }
    
        // Make sure the result is non-negative and within the range of total servers
        int totalServers = networkMap.size();
        return (hash & Integer.MAX_VALUE) % totalServers;
    }

    
    private boolean validateKey(String key) {
		// Check if KEY is greater than 0 bytes and not more than 24 bytes i.e. 12 characters in JAVA. We are not using any HEADER
		if (key.trim().length() == 0) {
			System.out.println("Invalid KEY.");
			return false;
		} else if (key.trim().length() > 10) {
			System.out.println("Invalid KEY. KEY should not be more than 24 bytes (12 characters).");
			return false;
		}
		return true;
	}

    private boolean validateValue(String value) {
        int minMaxLength = 500;
    
        if (value.trim().isEmpty() || value.length() > 2 * minMaxLength) {
            System.out.println("Invalid VALUE. VALUE should be between 1 and 1000 bytes.");
            return false;
        }
    
        return true;
    }
   
}



    
    
