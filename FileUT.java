
import java.io.File;
import java.net.SocketException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.net.Socket;
import java.util.ArrayList;
import java.nio.file.Files;


/*CS550 Advanced Operating Systems Programming Assignment 1 Repo
Illinois Institute of Technology

Team Name: KK Students:

Anirudha Kapileshwari (akapileshwari@hawk.iit.edu)
Mugdha Atul Kulkarni (mkulkarni2@hawk.iit.edu) 
*/




// file sharing methods
public class FileUT {

	private static final String dlocation = "downloads/";


	private static final String rlocation = FileTS.getReplicaloc();



	
	// featching file functon

	public static ArrayList<String> getF(String path) {
		
		ArrayList<String> files = new ArrayList<String>();
		
		File folder = new File(path);
		
		if (folder.isDirectory()) {
			File[] listOfFiles = folder.listFiles((dir, name) -> !name.endsWith("~") && new File(dir, name).isFile());
			
			
			if (listOfFiles != null) {
				
				for (File file : listOfFiles) 
				{
					files.add(file.getName());
				}
			}
		} else if (folder.isFile()) {
			files.add(path.substring(path.lastIndexOf("/") + 1));
		}
	
		return files;
	}
	
	//fetch file form location
	public static String getFLoc(String fileName, List<String> locations) {
		for (String path : locations) {
			
			File folder = new File(path);
			
			File[] listOfFiles = folder.listFiles();
	
			for (File file : listOfFiles) {
				if (file.getName().equals(fileName)) {
					return path.endsWith("/") ? path : path.concat("/");
				}
			}
		}
	
		return "File Not Found.";
	}
	
	// downlaod from peer

	public static boolean downloadF(String hostAddress, int port, String fileName, boolean fromReplica) {
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		Socket socket = null;
		boolean isDownloaded = false;
		
		try {
			// connect with peer to downlaod the file
			socket = new Socket(hostAddress, port);

			System.out.println("\nDownloading file form the Peer " + fileName);
			
			// folder ceation if not present
			File file = new File(dlocation);
			if (!file.exists())
				file.mkdir();

			// creation of output streem
			out = new ObjectOutputStream(socket.getOutputStream());
			
			out.flush();

			
			System.out.println("Requesting file from Peer.........");


			// REQ request object to dowlaod 


			REQ request = new REQ();
			request.setReqType(fromReplica ? "R_DOWNLOAD" : "DOWNLOAD");
			request.setReqData(fileName);
			out.writeObject(request);


			// Download file output stream
			System.out.println("Downloading file........");
			in = new ObjectInputStream(socket.getInputStream());
			
			
			file = new File(dlocation + fileName);
		
			byte[] bytes = (byte[]) in.readObject();	
			
			Files.write(file.toPath(), bytes);
			
			if((new File(dlocation + fileName)).length() == 0) {
				isDownloaded = false;
				(new File(dlocation + fileName)).delete();
			} else {
				isDownloaded = true;
			}




		} catch(SocketException e) {
			isDownloaded = false;
			
		} catch (Exception e) {
			isDownloaded = false;
			
		} finally {
			try {
				if (out != null) out.close();
				if (in != null) in.close();
				if (socket != null) socket.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return isDownloaded;
	}
	


	//replication of the file

	public static boolean replicateF(String hostAddress, int port, String fileName) {
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		
		Socket socket = null;
		boolean isReplicated = false;
		LogUT log = new LogUT("replication");
	
		try {
			long startTime = System.currentTimeMillis();
	
			// connect for file downloding
			socket = new Socket(hostAddress, port);
	
			
			File replicaFolder = new File(rlocation);
			if (!replicaFolder.exists()) {
				replicaFolder.mkdir();
			}
	
			
			out = new ObjectOutputStream(socket.getOutputStream());
			out.flush();
	
			log.writeMethod("Requesting file ... " + fileName);
			
			
			REQ request = new REQ();
			request.setReqType("DOWNLOAD");
			request.setReqData(fileName);
			out.writeObject(request);
	
			
			log.writeMethod("Downloading file ... " + fileName);
			in = new ObjectInputStream(socket.getInputStream());
	
			File outputFile = new File(rlocation + fileName);

			byte[] bytes = (byte[]) in.readObject();
			Files.write(outputFile.toPath(), bytes);
	
			long endTime = System.currentTimeMillis();
			double time = (double) Math.round(endTime - startTime) / 1000;
			log.writeMethod("File downloaded Complated in " + time + " seconds.");
			isReplicated = true;
		} catch (SocketException e) {
			log.writeMethod("connection Error with host, File not Downloaded");
			log.writeMethod("Error: " + e);
		} catch (Exception e) {
			log.writeMethod("error in downloading file check file properties(write)");
			log.writeMethod("Error: " + e);
		}
		
		finally {
			try {
				// Closing streams
				if (out != null) out.close();
				
				if (in != null) in.close();
				
				if (socket != null) socket.close();
				
				if (log != null) log.closelog();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return isReplicated;
	}



	//Delete File Fnc
	public static boolean deleF(String fileName) {
		File file = new File(rlocation + fileName);
		if (file.exists()) {
			return file.delete();
		}
		return false;
	}
	
	
}