
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Calendar;
import java.io.FileWriter;




// log handling class

public class LogUT {

	
	private final String logloc = "logs/";
	private BufferedWriter writer = null;
	private String logF = "";

	

	public LogUT(String logType) {
		try {
			String logF = getLogFileName(logType);  // Get the log file name based on logType
			createLogsFolder();  // Create a "logs" folder if it doesn't exist
	
			writer = new BufferedWriter(new FileWriter(logloc + logF, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private String getLogFileName(String logType) {
		switch (logType.toLowerCase()) {
			case "peer":
				logF = "peer.server.log";
				return "peer.server.log";
			case "server":
				logF = "server.log";
				return "server.log";
			case "replication":
				logF = "replication.log";
				return "replication.log";
			default:
				throw new IllegalArgumentException("Invalid logType: " + logType);
		}
	}
	
	private void createLogsFolder() {
		File file = new File(logloc);
		if (!file.exists()) {
			if (file.mkdir()) {
				System.out.println("Logs folder created.");
			} else {
				System.err.println("Failed to create logs folder.");
			}
		}
	}
	

	


	public boolean writeMethod(String logText) {
		try {
			if (writer != null) {
				String timestamp = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
				String formattedLog = String.format("%s => %s%n", timestamp, logText);
				writer.write(formattedLog);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	
	// Print method
	public void Logprint() {
		
		
		
		BufferedReader buffer = null;
		

		
		System.out.println("\nLOG");
		System.out.println("-----------------------------------------------");
		try {
			buffer = new BufferedReader(new FileReader(logloc + logF));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int Count = 0;
			String lineoflog = null;
			try {
				
				while ((lineoflog = buffer.readLine()) != null) {
					System.out.println(lineoflog);
					
					Count = Count + lineoflog.length();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		
		if (Count == 0) {
			System.out.println("NO LOGS TO PRINT");
		}

		System.out.println("------------------------------------------------");
	}
	
	// close the log
	public void closelog() throws IOException {
			if (writer != null) {
				String newline = System.getProperty("line.separator");
				writer.write(newline);
				writer.close();
			}
		
	}

	//destroy
	@Override
	protected void finalize() throws Throwable {
		if (writer != null) writer.close();
		
	}
	
	
}
