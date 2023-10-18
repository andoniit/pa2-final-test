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

    private HashMap<Integer, String> netMap=null;
    private ArrayList<String> repliNodes= null;

    private int portAddr=0;
    private String filesLoc=null;
    private String localAddr=null;
    

    public pc(){
        netMap=FileTS.getNetWM();
        localAddr=FileTS.getLocAddr();
        filesLoc=FileTS.getFileloc();
        repliNodes=FileTS.getRNodes();
        portAddr=FileTS.getpSPort();
        
    }

    //thread implementation for peer to serve as client

    public void run() {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in))) {
            HashMap<String, String> hm = retrieveHT();
            if (hm != null) {
                for (final Map.Entry e : hm.entrySet()) {
                    FileTS.putinHT(e.getKey().toString(), e.getValue().toString(), true);
                }
            }
            
            if (repliNodes.contains(localAddr)) {
                System.out.println("Replication server Started......");
                replication service = new replication(null, null, "REPLICATE");
                service.start();
            }
            
            while (true) {
                System.out.println("\nWelocme to File Sharing System!!!!\n Select a Operation to perfrom");
                System.out.println("1.Upload A file to File Sharing System");
                System.out.println("2.Browose for a file in File Sharing System");
                System.out.println("3.Remove a File from File Sharing System");
                System.out.println("4.Display Log-File of this Peer");
                System.out.println("5.Exit.");
                System.out.print("Enter Number of operation and press ENTER:");
                
                int option;
                try {
                    option = Integer.parseInt(input.readLine());
                } catch (NumberFormatException e) {
                    System.out.println("Please choose correct Operation ");
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
                        System.out.println("Please choose correct Operation ");
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
        System.out.println("\nEnter The full name of the file with file type(eg-andon.pdf)");
        String inputfileName = input.readLine();
        
        if (inputfileName.trim().length() == 0) {
            System.out.println("Error in file Name........");
            return;
        }
        //////////
        File file = new File(filesLoc + inputfileName);
        
        if (file.exists()) {
            long startTime = System.currentTimeMillis();
            if (uploadF(inputfileName, localAddr)) {
                System.out.println(inputfileName + " Upload is complated............");
            } else {
                System.out.println("error in adding " + inputfileName + " to the file sharing system.");
            }
            double timetakentoupload = (double) Math.round(System.currentTimeMillis() - startTime) / 1000;
            System.out.println("Time Taken: " + timetakentoupload + " seconds.");
        } else {
            System.out.println("Location error [" + filesLoc + "] path.");
        }
    }
    
    
    //search for a file
    private void searchForFile(BufferedReader input) throws IOException {
        System.out.println("\nEnter name of FILE to Brows in the SYSTEM");
        String fileName = input.readLine();
        String hostAddress;
        
        long startTime = System.currentTimeMillis();
        String value = Brows(fileName);
        double time = (double) Math.round(System.currentTimeMillis() - startTime) / 1000;
        
        if (value != null) {
            System.out.println("File Available to downlaod. search time: " + time + " seconds.");
            hostAddress = value;
            
            System.out.print("\nDo you want to download this file? (Y/N):");
            String download = input.readLine();

            if (download.equalsIgnoreCase("Y")) {
                    downlaodfile(hostAddress, portAddr, fileName);
                    System.out.print("\nFile DOWNLOAD Successful");
                }else{
                    System.out.print("\nSearch File complated");
                }
            
        } else {
            System.out.println("File not found. Lookup time: " + time + " seconds.");
        }
    }
    
    
    //unregister a file
    private void unregisterFile(BufferedReader input) throws IOException {
        System.out.println("\n File name to D-Register from SYSTEM");
        String key = input.readLine();
        
        if (!validateKey(key)) {
            return;
        }
        
        System.out.print("\nAre you sure (Y/N)?:");
        String confirm = input.readLine();
        
        if (confirm.equalsIgnoreCase("Y")) {
            long startTime = System.currentTimeMillis();
            if (deletefromsystem(key)) {
                System.out.println("File Removed from SYSTEM......");
            } else {
                System.out.println("Error while Removing File from System");
            }
            double time = (double) Math.round(System.currentTimeMillis() - startTime) / 1000;
            System.out.println("Time taken: " + time + " seconds");
        }
    }
    
    
    //print log
    private void printLog() {
        (new LogUT("peer")).Logprint();
    }
    
    
    //exit peer
    private void exitPeer(BufferedReader input) throws IOException {
        System.out.print("\n Selecting 'Y' will remove this IP from SYSTEM (Y/N)?:");
        String confirm = input.readLine();
        
        if (confirm.equalsIgnoreCase("Y")) {
            System.out.println("File Sharing System Turned OFF........");
            System.exit(0);
        }


    }


    //download file
    private void downlaodfile(String hostAddress, int port, String fileName) {
        long startTime = System.currentTimeMillis();
        boolean isDownloaded = false;
    
        if (FileUT.downloadF(hostAddress, port, fileName, false)) {
            isDownloaded = true;
        } else {
            List<String> backupNodes = FileTS.getRNodes();
    
            for (String node : backupNodes) {
                if (FileUT.downloadF(node, port, fileName, true)) {
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
            System.out.println("Undfeined Error");
        }
    }



//retrives hashtable from one of the replication nodes
    private HashMap<String, String> retrieveHT() {
    HashMap<String, String> hm = null;

    for (String nodeAddress : repliNodes) {
        if (nodeAddress.equalsIgnoreCase(localAddr)) {
            continue;
        }

        try (Socket socket = new Socket(nodeAddress, portAddr);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            REQ peerRequest = new REQ();
            peerRequest.setReqType("GET_R_HASHTABLE");
            out.writeObject(peerRequest);

            RES serverResponse = (RES) in.readObject();
            if (serverResponse != null && serverResponse.getRespCd() == 200) {
                hm = (HashMap<String, String>) serverResponse.getRespData();
                break;
            }
        } catch (Exception ex) {
            ex.printStackTrace(); // Handle the exception appropriately.
        }
    }

    
    return hm;
}


    //this method is used to register a file on a network
    private boolean uploadF(String key, String value) {
        try {
            int node = hashID(key);
            String nodeAddress = netMap.get(node);
    
            if (nodeAddress.equals(localAddr)) {
                if (FileTS.getHT().containsKey(key)) {
                    System.out.print("\nAlready available,Overight? (Y/N):  ");
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
                try (Socket socket = new Socket(nodeAddress, portAddr);
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
    
                    String data = key + "," + value;
                    REQ peerRequest = new REQ();
                    peerRequest.setReqType("REGISTER");
                    peerRequest.setReqData(data);
                    out.writeObject(peerRequest);
    
                    RES serverResponse = (RES) in.readObject();
    
                    if (serverResponse.getRespCd() == 200) {
                        return true;
                    } else if (serverResponse.getRespCd() == 300) {
                        System.out.print("\n Already available,Overight? (Y/N): ");
                        String confirm = (new BufferedReader(new InputStreamReader(System.in))).readLine();
    
                        if (confirm.equalsIgnoreCase("Y")) {
                            return forceupload(key, value);
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
    private boolean forceupload(String key, String value) {
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
    
            int node = hashID(key);
            String nodeAddress = netMap.get(node);
    
            if (nodeAddress.equals(localAddr)) {
                boolean result = FileTS.getHT().containsKey(key);
    
                if (result) {
                    System.out.print("\nAlready available,Overight? (Y/N):  ");
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
                socket = new Socket(nodeAddress, portAddr);
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
    
                peerRequest = new REQ();
                peerRequest.setReqType("REGISTER_FORCE");
                peerRequest.setReqData(data);
                out.writeObject(peerRequest);
    
                serverResponse = (RES) in.readObject();
    
                if (serverResponse.getRespCd() == 200) {
                    endTime = System.currentTimeMillis();
                    time = (double) Math.round(endTime - startTime) / 1000;
                    System.out.println("Time taken: " + time + " seconds");
                    return true;
                } else {
                    System.out.print(serverResponse.getRespData());
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
    private String Brows(String key) {
        String value = null;
        RES serverResponse = null;
        Socket socket = null;
         ObjectOutputStream out = null;
        REQ peerRequest = null;

        String nodeAddress=null;
        ObjectInputStream in = null;
       
        
        try {
            int node = hashID(key);
            nodeAddress = netMap.get(node);
    
            if (nodeAddress.equals(localAddr)) {
                value = FileTS.getfromHT(key);
            } else {
                socket = new Socket(nodeAddress, portAddr);
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
    
                peerRequest = new REQ();
                peerRequest.setReqType("LOOKUP");
                peerRequest.setReqData(key);
                out.writeObject(peerRequest);
    
                serverResponse = (RES) in.readObject();
    
                if (serverResponse.getRespCd() == 200) {
                    String[] responseData = serverResponse.getRespData().toString().split(",");
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
            value = RepiSearch(key, nodeAddress);
        }
    
        return value;
    }


    //this function searches for a file on a replica
    private String RepiSearch(String key, String peerAddress) {
        String value = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;


        REQ peerRequest = null;
        RES serverResponse = null;
        Socket socket = null;
        
    
        for (String nodeAddress : repliNodes) {
            try {
                if (nodeAddress.equals(localAddr)) {
                    value = FileTS.getfromREP_HT(key);
                } else {
                    socket = new Socket(nodeAddress, portAddr);
                    out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    in = new ObjectInputStream(socket.getInputStream());
    
                    peerRequest = new REQ();
                    peerRequest.setReqType("R_LOOKUP");
                    peerRequest.setReqData(key);
                    out.writeObject(peerRequest);
    
                    serverResponse = (RES) in.readObject();
    
                    if (serverResponse.getRespCd() == 200) {
                        value = serverResponse.getRespData().toString().split(",")[1].trim();
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
    private boolean deletefromsystem(String key) {
        Socket socket = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        REQ peerRequest = null;
        RES serverResponse = null;
        String nodeAddress=null;
        
        int node = hashID(key);
        nodeAddress = netMap.get(node);
    
        try {
            
            socket = new Socket(nodeAddress, portAddr);
            
            
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            
            
            in = new ObjectInputStream(socket.getInputStream());
    
            
            peerRequest = new REQ();
            peerRequest.setReqType("UNREGISTER");
            peerRequest.setReqData(key);
            out.writeObject(peerRequest);
            
            
            serverResponse = (RES) in.readObject();
    
            if (serverResponse.getRespCd() == 200) {
                return true;
            } else {
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            
            e.printStackTrace();
        } finally {
            try {
                
                if (out != null) out.close();
                
                if (in != null) in.close();
                
                if (socket != null) socket.close();
            } 
            
            
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        return false;
    }

    private int hashID(String key) {
        int hash = 5381; 
    
        for (int i = 0; i < key.length(); i++) {
            hash = (hash * 33) ^ key.charAt(i); 
        }
    
        

        int totalServers = netMap.size();
        return (hash & Integer.MAX_VALUE) % totalServers;
    }

    
    private boolean validateKey(String key) {
		
		if (key.trim().length() == 0) {
			System.out.println("Invalid KEY.");
			return false;
		}
        
        
        else if (key.trim().length() > 10) {
			System.out.println("Invalid KEY. KEY should not be more than 24 bytes (12 characters).");
			return false;
		}
		return true;
	}

    
   
}



    
    
