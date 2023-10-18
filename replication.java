
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class replication extends Thread{

    private ArrayList<String> repliNodes= null;
    private int portNumber;
    private String localAddr=null;
    private HashMap<Integer, String> netMap= null;

    private String dKey=null;
    private String dValue=null;
    private String reqType=null;
    int portAddress=0;

    public replication(String key,String value, String requestType){

        repliNodes=FileTS.getRNodes();
        portNumber=FileTS.getpSPort();
        localAddr=FileTS.getLocAddr();
        netMap=FileTS.getNetWM();

        this.dKey=key;
        this.dValue=value;
        this.reqType=requestType;

    }

    public void run() {
       
        String data = this.dKey + "," + this.dValue;
        
    
        if (reqType.equalsIgnoreCase("REPLICATE")) {
            replicateHashTables();
            replicateFiles();
            this.interrupt();
            return;
        }
    
        for (String nodeAddress : repliNodes) {
            try {
                if (reqType.equalsIgnoreCase("REGISTER")) {
                    if (nodeAddress.equalsIgnoreCase(localAddr)) {
                        // Handle the local register operation
                        LogUT log = new LogUT("peer");
                        log.writeMethod(String.format("Serving REPLICATE - REGISTER(%s,%s) request of %s.", this.dKey, this.dValue, localAddr));
                        FileTS.putinREP_HT(nodeAddress, this.dKey, this.dValue);
                        replicate(this.dValue, portAddress, this.dKey);
                        log.writeMethod(String.format("REPLICATE - REGISTER(%s,%s) for %s completed successfully.", this.dKey, this.dValue, localAddr));
                        log.closelog();
                    } else {
                        // Handle the remote register operation
                        handleRegisterOrUnregister(nodeAddress, "R_REGISTER", data);
                    }
                } else if (reqType.equalsIgnoreCase("UNREGISTER")) {
                    // Handle the unregister operation
                    handleRegisterOrUnregister(nodeAddress, "R_UNREGISTER", this.dKey);
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
            peerRequest.setReqType(requestType);
            peerRequest.setReqData(requestData);
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
		if (repliNodes.size() > 1) {
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
        FileUT.replicateF(hostAddress, port, fileName);
    }


    //retrives replication hash tables from replication nodes
    private ConcurrentHashMap<String, HashMap<String, String>> getReplicationData() {
    for (String nodeAddress : repliNodes) {
        if (nodeAddress.equalsIgnoreCase(localAddr)) {
            continue;
        }

        try (Socket socket = new Socket(nodeAddress, portAddress);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            REQ peerRequest = new REQ();
            peerRequest.setReqType("GET_REPLICA");
            out.writeObject(peerRequest);

            RES serverResponse = (RES) in.readObject();
            if (serverResponse != null && serverResponse.getRespCd() == 200) {
                return (ConcurrentHashMap<String, HashMap<String, String>>) serverResponse.getRespData();
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

    for (Map.Entry<Integer, String> peer : netMap.entrySet()) {
        if (peer.getValue().equalsIgnoreCase(localAddr)) {
            if (FileTS.getHT().size() > 0) {
                replicatedHashTable.put(peer.getValue(), new HashMap<>(FileTS.getHT()));
            }
            continue;
        }

        try (Socket socket = new Socket(peer.getValue(), portAddress);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            REQ peerRequest = new REQ();
            peerRequest.setReqType("GET_HASHTABLE");
            out.writeObject(peerRequest);

            RES serverResponse = (RES) in.readObject();
            if (serverResponse != null && serverResponse.getRespCd() == 200) {
                ConcurrentHashMap<String, String> hm = (ConcurrentHashMap<String, String>) serverResponse.getRespData();
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