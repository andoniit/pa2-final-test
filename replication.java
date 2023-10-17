import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class replication extends Thread{

    private ArrayList<String> replicationNodes= null;
    private int portNumber;
    private String localAddress=null;
    private HashMap<Integer, String> networkMap= null;

    private String dataKey=null;
    private String dataValue=null;
    private String requestType=null;
    int portAddress=0;

    public replication(String key,String value, String requestType){

        replicationNodes=FileTS.getRNodes();
        portNumber=FileTS.getpSPort();
        localAddress=FileTS.getLocAddr();
        networkMap=FileTS.getNetWM();

        this.dataKey=key;
        this.dataValue=value;
        this.requestType=requestType;

    }

    public void run() {
       
        String data = this.dataKey + "," + this.dataValue;
        
    
        if (requestType.equalsIgnoreCase("REPLICATE")) {
            replicateHashTables();
            replicateFiles();
            this.interrupt();
            return;
        }
    
        for (String nodeAddress : replicationNodes) {
            try {
                if (requestType.equalsIgnoreCase("REGISTER")) {
                    if (nodeAddress.equalsIgnoreCase(localAddress)) {
                        // Handle the local register operation
                        LogUT log = new LogUT("peer");
                        log.write(String.format("Serving REPLICATE - REGISTER(%s,%s) request of %s.", this.dataKey, this.dataValue, localAddress));
                        FileTS.putinREP_HT(nodeAddress, this.dataKey, this.dataValue);
                        replicate(this.dataValue, portAddress, this.dataKey);
                        log.write(String.format("REPLICATE - REGISTER(%s,%s) for %s completed successfully.", this.dataKey, this.dataValue, localAddress));
                        log.closelog();
                    } else {
                        // Handle the remote register operation
                        handleRegisterOrUnregister(nodeAddress, "R_REGISTER", data);
                    }
                } else if (requestType.equalsIgnoreCase("UNREGISTER")) {
                    // Handle the unregister operation
                    handleRegisterOrUnregister(nodeAddress, "R_UNREGISTER", this.dataKey);
                }
            } catch (Exception ex) {
                // Handle exceptions or log errors.
            }
        }
        this.interrupt();
    }
    
    private void handleRegisterOrUnregister(String nodeAddress, String requestType, String requestData) {
        // Make connection with the server using the specified Host Address and Port 10000
        try (Socket socket = new Socket(nodeAddress, portAddress);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
    
            REQ peerRequest = new REQ();
            peerRequest.setRequestType(requestType);
            peerRequest.setRequestData(requestData);
            out.writeObject(peerRequest);
    
            // Read the response message from the server
            RES serverResponse = (RES) in.readObject();
        } catch (Exception e) {
            // Handle exceptions or log errors.
        }
    }


    //retrives data from hash table
    private boolean replicateHashTables() {
		ConcurrentHashMap<String, HashMap<String, String>> replicatedHashTable = null;		
		if (replicationNodes.size() > 1) {
			// If there is another replication node, get replication data from another data
			replicatedHashTable = getReplicationData();
		} else {
			// If there is only one replication node. It gets data from all the peers in the network.
			replicatedHashTable = getAllHashTables();
		}
		
		if (replicatedHashTable != null && replicatedHashTable.size() > 0) {
			FileTS.setReplicatedHT(replicatedHashTable);
			return true;
		}
		return false;
	}

    private void replicateFiles() {
        FileTS.getReplicatedHT().forEach((replicaPeerAddress, hashTable) -> {
            hashTable.forEach((fileName, peerAddress) -> {
                replicate(peerAddress, portAddress, fileName);
            });
        });
    
        this.interrupt();
    }
    
    
    //download file from requested peer
    private void replicate(String hostAddress, int port, String fileName) {
        FileUT.replicateFile(hostAddress, port, fileName);
    }


    //retrives replication hash tables from replication nodes
    private ConcurrentHashMap<String, HashMap<String, String>> getReplicationData() {
    for (String nodeAddress : replicationNodes) {
        if (nodeAddress.equalsIgnoreCase(localAddress)) {
            continue;
        }

        try (Socket socket = new Socket(nodeAddress, portAddress);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            REQ peerRequest = new REQ();
            peerRequest.setRequestType("GET_REPLICA");
            out.writeObject(peerRequest);

            RES serverResponse = (RES) in.readObject();
            if (serverResponse != null && serverResponse.getResponseCode() == 200) {
                return (ConcurrentHashMap<String, HashMap<String, String>>) serverResponse.getResponseData();
            }
        } catch (Exception ex) {
            // Handle or log the exception as needed
        }
    }

    return null;
}


//this method requests all the peers in their networks to send their hash tables
private ConcurrentHashMap<String, HashMap<String, String>> getAllHashTables() {
    ConcurrentHashMap<String, HashMap<String, String>> replicatedHashTable = new ConcurrentHashMap<>();

    for (Map.Entry<Integer, String> peer : networkMap.entrySet()) {
        if (peer.getValue().equalsIgnoreCase(localAddress)) {
            if (FileTS.getHT().size() > 0) {
                replicatedHashTable.put(peer.getValue(), new HashMap<>(FileTS.getHT()));
            }
            continue;
        }

        try (Socket socket = new Socket(peer.getValue(), portAddress);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            REQ peerRequest = new REQ();
            peerRequest.setRequestType("GET_HASHTABLE");
            out.writeObject(peerRequest);

            RES serverResponse = (RES) in.readObject();
            if (serverResponse != null && serverResponse.getResponseCode() == 200) {
                ConcurrentHashMap<String, String> hm = (ConcurrentHashMap<String, String>) serverResponse.getResponseData();
                if (hm.size() > 0) {
                    replicatedHashTable.put(peer.getValue(), new HashMap<>(hm));
                }
            }
        } catch (Exception e) {
            // Handle or log the exception as needed
        }
    }

    return replicatedHashTable;
}

    
}