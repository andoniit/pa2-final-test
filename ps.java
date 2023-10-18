import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.nio.file.Files;



public class ps extends Thread {
    private Socket socket;
    private LogUT log;
    private String filesLocation;
    private int portAddress;
    private String replicaLocation;
 
    public ps (Socket socket) {
        this.socket = socket;
        log = new LogUT("peer");
        log.writeMethod("Connected with " + socket.getInetAddress() + ".");
        filesLocation = FileTS.getFileloc();
        portAddress = FileTS.getpSPort();
        replicaLocation = FileTS.getReplicaloc();
    }

    public void run() {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            REQ request = (REQ) in.readObject();
            //RES response = null;

            if (request.getReqT().startsWith("REGISTER")) {
                handleRegisterRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("LOOKUP")) {
                handleLookupRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("UNREGISTER")) {
                handleUnregisterRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("DOWNLOAD")) {
                handleDownloadRequest(request, out, filesLocation);
            } else if (request.getReqT().equalsIgnoreCase("R_DOWNLOAD")) {
                handleDownloadRequest(request, out, replicaLocation);
            } else if (request.getReqT().equalsIgnoreCase("R_REGISTER")) {
                handleReplicateRegisterRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("R_LOOKUP")) {
                handleReplicateLookupRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("R_UNREGISTER")) {
                handleReplicateUnregisterRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("GET_R_HASHTABLE")) {
                handleGetReplicaHashTableRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("GET_HASHTABLE")) {
                handleGetHashTableRequest(request, out);
            } else if (request.getReqT().equalsIgnoreCase("GET_REPLICA")) {
                handleGetReplicationDataRequest(request, out);
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                log.closelog();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void handleRegisterRequest(REQ request, ObjectOutputStream out) throws IOException {
        String data = (String) request.getReqData();
        String key = data.split(",")[0];
        String value = data.split(",")[1];
        boolean result;
        RES res=null;

        log.writeMethod(String.format("Serving REGISTER(%s,%s) request of %s.", key, value, socket.getInetAddress().getHostAddress()));

        if (request.getReqT().endsWith("FORCE")) {
            result = FileTS.putinHT(key, value, true);
        } else {
            result = FileTS.putinHT(key, value, false);
        }

        if (result) {
            res = new RES();
            res.setRespCd(200);
            res.setRespData("(Key,Value) pair added successfully.");
            out.writeObject(res);

            log.writeMethod(String.format("REGISTER(%s,%s) for %s completed successfully.", key, value, socket.getInetAddress().getHostAddress()));

            replication service = new replication(key, value, "REGISTER");
            service.start();
        } else {
            res = new RES();
            res.setRespCd(300);
            res.setRespData("Value with this KEY already exists.");
            out.writeObject(res);

            log.writeMethod(String.format("REGISTER(%s,%s) for %s failed. KEY already exists.", key, value, socket.getInetAddress().getHostAddress()));
        }
    }

    private void handleLookupRequest(REQ request, ObjectOutputStream out) throws IOException {
        String key = (String) request.getReqData();
        RES res=null;

        log.writeMethod(String.format("Serving LOOKUP(%s) request of %s.", key, socket.getInetAddress().getHostAddress()));
        String value = FileTS.getfromHT(key);

        res = new RES();
        if (value != null) {
            res.setRespCd(200);
            res.setRespData(key + "," + value);
            log.writeMethod(String.format("LOOKUP(%s) = %s for %s completed successfully.", key, value, socket.getInetAddress().getHostAddress()));
        } else {
            res.setRespCd(404);
            res.setRespData("VALUE with this KEY does not exist.");
            log.writeMethod(String.format("LOOKUP(%s) = %s for %s completed successfully. Key not found.", key, value, socket.getInetAddress().getHostAddress()));
        }

        out.writeObject(res);
    }

    private void handleUnregisterRequest(REQ request, ObjectOutputStream out) throws IOException {
        String key = (String) request.getReqData();
        RES res=null;

        log.writeMethod(String.format("Serving UNREGISTER(%s) request of %s.", key, socket.getInetAddress().getHostAddress()));
        FileTS.removefromHT(key);

        res = new RES();
        res.setRespCd(200);
        out.writeObject(res);

        log.writeMethod(String.format("UNREGISTER(%s) for %s completed successfully.", key, socket.getInetAddress().getHostAddress()));

        replication service = new replication(key, null, "UNREGISTER");
        service.start();
    }

    private void handleDownloadRequest(REQ request, ObjectOutputStream out, String location) throws IOException {
        
        String fileName = (String) request.getReqData();
        log.writeMethod("Uploading/Sending file " + fileName);

        File file = new File(location + fileName);
        byte[] fileBytes = Files.readAllBytes(file.toPath());
        out.writeObject(fileBytes);
        out.flush();
        log.writeMethod("File sent successfully.");
    }

    private void handleReplicateRegisterRequest(REQ request, ObjectOutputStream out) throws IOException {
    String data = (String) request.getReqData();
    String key = data.split(",")[0];
    String value = data.split(",")[1];
    RES res=null;

    log.writeMethod(String.format("Serving REPLICATE - REGISTER(%s,%s) request of %s.", key, value, socket.getInetAddress().getHostAddress()));

    FileTS.putinREP_HT(socket.getInetAddress().getHostAddress(), key, value);

    res = new RES();
    res.setRespCd(200);
    res.setRespData("(Key,Value) pair added successfully.");
    out.writeObject(res);

    FileUT.replicateF(value, portAddress, key);

    log.writeMethod(String.format("REPLICATE - REGISTER(%s,%s) for %s completed successfully.", key, value, socket.getInetAddress().getHostAddress()));
}

private void handleReplicateLookupRequest(REQ request, ObjectOutputStream out) throws IOException {
    String key = (String) request.getReqData();
    RES res=null;

    log.writeMethod(String.format("Serving REPLICATE - LOOKUP(%s) request of %s.", key, socket.getInetAddress().getHostAddress()));

    String value = FileTS.getfromREP_HT(key);

    res = new RES();
    if (value != null) {
        res.setRespCd(200);
        res.setRespData(key + "," + value);
        log.writeMethod(String.format("REPLICATE - LOOKUP(%s) = %s for %s completed successfully.", key, value, socket.getInetAddress().getHostAddress()));
    } else {
        res.setRespCd(404);
        res.setRespData("VALUE with this KEY does not exist.");
        log.writeMethod(String.format("REPLICATE - LOOKUP(%s) = %s for %s completed successfully. Key not found.", key, value, socket.getInetAddress().getHostAddress()));
    }

    out.writeObject(res);
}

private void handleReplicateUnregisterRequest(REQ request, ObjectOutputStream out) throws IOException {
    String key = (String) request.getReqData();
    RES res=null;

    log.writeMethod(String.format("Serving REPLICATE - UNREGISTER(%s) request of %s.", key, socket.getInetAddress().getHostAddress()));

    FileTS.removefromREP_HT(socket.getInetAddress().getHostAddress(), key);

    res = new RES();
    res.setRespCd(200);
    out.writeObject(res);

    if (FileUT.deleF(key)) {
        log.writeMethod(String.format("File %s deleted.", key));
    } else {
        log.writeMethod(String.format("File %s could not be deleted.", key));
    }

    log.writeMethod(String.format("REPLICATE - UNREGISTER(%s) for %s completed successfully.", key, socket.getInetAddress().getHostAddress()));
}

private void handleGetReplicaHashTableRequest(REQ request, ObjectOutputStream out) throws IOException {
    RES res=null;
    log.writeMethod(String.format("Serving GET_R_HASHTABLE request of %s.", socket.getInetAddress().getHostAddress()));

    HashMap<String, String> innerMap = FileTS.getReplicatedHT().get(socket.getInetAddress().getHostAddress());

    res = new RES();
    if (innerMap != null) {
        res.setRespCd(200);
        res.setRespData(innerMap);
        out.writeObject(res);
    } else {
        res = new RES();
        res.setRespCd(404);
        out.writeObject(res);
    }

    log.writeMethod(String.format("DATA of %s sent successfully. Request completed. " + innerMap, socket.getInetAddress().getHostAddress()));
}

private void handleGetHashTableRequest(REQ request, ObjectOutputStream out) throws IOException {
    RES res=null;
    log.writeMethod(String.format("Serving GET_HASHTABLE request of %s.", socket.getInetAddress().getHostAddress()));

    res = new RES();
    res.setRespCd(200);
    res.setRespData(FileTS.getHT());
    out.writeObject(res);

    log.writeMethod(String.format("HASH TABLE sent to %s successfully. Request completed. " + FileTS.getReplicatedHT(), socket.getInetAddress().getHostAddress()));
}

private void handleGetReplicationDataRequest(REQ request, ObjectOutputStream out) throws IOException {
    RES res=null;
    log.writeMethod(String.format("Serving GET_REPLICA request of %s.", socket.getInetAddress().getHostAddress()));

    res = new RES();
    res.setRespCd(200);
    res.setRespData(FileTS.getReplicatedHT());
    out.writeObject(res);

    log.writeMethod(String.format("REPLCATION DATA sent to %s successfully. Request completed. " + FileTS.getReplicatedHT(), socket.getInetAddress().getHostAddress()));
}


    @Override
    public void interrupt() {
        try {
            log.closelog();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        super.interrupt();
    }
}

