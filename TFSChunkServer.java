import java.util.*;
import java.io.*;
import java.net.*;

public class TFSChunkServer {

	//Where in local are we storing all files - default location
	String location = "C:\\java_progs\\img\\";
	//Relating handles and files
	Map<Long, byte[]> mapHandleFile = new HashMap<Long, byte[]>();
	//Relating handles and file names in local folder
	Map<Long, String> mapHandlePath = new HashMap<Long, String>();
	//64-bit handles for files
	long nextAvailableHandle = 0;	
	int chunkServerID = 0;
	static int nextAvailableChunkServerID = 0;
	
	//HACK: BOOLEAN FOR NOW
	boolean Error = false;
	
	
	TFSMessage outgoingMessage = new TFSMessage();
	
	public TFSChunkServer() {
		chunkServerID = nextAvailableChunkServerID;
		//location = fileLocation;
		nextAvailableChunkServerID++;
	}
	
	public int getID() {
		return chunkServerID;
	}
	
	public int numOfFile() {
		return mapHandleFile.size();
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
        
		TFSChunkServer chunkserver = new TFSChunkServer();
		
        if (args.length != 1) {
            System.err.println("Usage: java EchoServer <port number>");
            System.exit(1);
        }
        
        int portNumber = Integer.parseInt(args[0]);
        
        try (
            ServerSocket serverSocket =
                new ServerSocket(Integer.parseInt(args[0]));
            Socket clientSocket = serverSocket.accept();     
            PrintWriter out_print =
                new PrintWriter(clientSocket.getOutputStream(), true); //Sends the String back to client to print, can change this to ObjectOutput to send messages
			//Receive messages
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()); 
			//Send Messages
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
        ) {
			//int messageCount = 0; //Just a hack to keep it from closing the connection. Probably need to throw all this into the "run" method
			//This is to basically store in the hashmap whatever files are already in the folder
			chunkserver.retrieveFiles();
			//System.out.println(chunkserver.numOfFile());
			while (true){
				TFSMessage incomingMessage = new TFSMessage(); //create a new MessageObject, I think we should have the constructor set everything to null when it's initialized
				chunkserver.outgoingMessage.clearMsg();
			
				incomingMessage.receiveMessage(in); //receive Messages - call readObject 
				
				//HACK: STILL FIGURING OUT WHAT NAME IS FOR
				String content = "ChunkServer " + chunkserver.getID() + ": " + incomingMessage.getName();
				System.out.println(content);
				//pull the changed name in our new object
				
				
				if (content != null){ //if we received data
					//HACK: STILL NEED A SWITCH STATEMENT HERE
					
					//chunkserver.storeTest(incomingMessage.getFile());
					
					int receivedHandle = 0;
					//Error = chunkserver.retrieveTest(receivedHandle);
					//chunkserver.retrieveTest(receivedHandle);
				
					//chunkserver.deleteTest(receivedHandle);
					
					chunkserver.send(out);
				
					
				}
			}
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
		}
	}
	
	public void send(ObjectOutputStream o) throws IOException {
		String content = "ChunkServer: " + outgoingMessage.getName();
		System.out.println(content);
		outgoingMessage.sendMessage(o);
	}
		
	//Store files in the TFS
	public void storeTest(byte[] b) {
		//HACK: STILL NEED TO KNOW WHERE TO STORE IT - HERE FIRST - how to know what to name file
		String fileName = "textone.jpg";
		
		String newLocation = location + fileName;
		try{
			//Creating a new file
			File f = new File(newLocation);
			//File already exists
			if(f.exists()) {
				System.out.println("ChunkServer " + chunkServerID + ": Error, file already exists in local.");
				
				//Setting up outgoing message
				outgoingMessage.setName("Storing file in TFS incomplete");
				outgoingMessage.ImChunk();
			}
			else {
				//Opening local location and writing/adding file to it
				FileOutputStream fileOutputStream = new FileOutputStream(newLocation);
				fileOutputStream.write(b);
				fileOutputStream.close();
				
				//Since adding file to local is successful need to add to map
				mapHandleFile.put(nextAvailableHandle, b);
				mapHandlePath.put(nextAvailableHandle,fileName);
				nextAvailableHandle++;
				
				//Setting up outgoing message
				outgoingMessage.setName("Storing file in TFS complete");
				outgoingMessage.ImChunk();
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Retrieve Files
	public void retrieveFiles() {
		/** 
			chunkServer checks if there are any files in its local directory/folder and assign IDs to these files. It is up to the master to decide if these files are the latest files.
			
			This is specifically for when the chunkServer dies; it will need to recover its files.
		**/
		File folder = new File(location);
		File[] listOfFiles = folder.listFiles();
		
		for(File file : listOfFiles) {
			if(file.isFile()) {
				String newLocation = location + file.getName();
				try {
					RandomAccessFile f = new RandomAccessFile(file, "r");
					byte[] b = new byte[(int)f.length()];
					f.read(b);
					mapHandleFile.put(nextAvailableHandle, b);
					mapHandlePath.put(nextAvailableHandle, file.getName());
					nextAvailableHandle++;
					System.out.println("File name is: " + file.getName());
				}
				catch (IOException e) {
					System.out.println("Can't locate local file path.");
					e.printStackTrace();
				}
			}
		}
	}
	
	//HACK: BOOLEAN MAY NOT BE NEEDED, BUT IF IT IS I THINK IT WOULD BE NICE TO DO IT FOR EVERYTHING TO SET FOR ERROR VALUE IN MESSAGE
	public void retrieveTest(long fileHandle) {
		//Since all files in the folder were retrieved in start of chunkserver and all files added are in the map as well, there is no need to read for local files to send the byte array to client
		byte[] b;
		b = mapHandleFile.get(fileHandle);
		if(b != null) {
			//Setting up the outgoing message
			//outgoingMessage.clearMsg();
			outgoingMessage.setName("Retrieve file from TFS request");
			outgoingMessage.ImChunk();
			outgoingMessage.setFile(b);
			//return true;
		}
		else {
			//Error fileHandle is not in the map
			System.out.println("Error: chunkHandle does not exist");
			//return false;
		}
			
	}
	
	//WAIT: are we allowed to append if file didn't exist?
	// public void appendTest(byte[] b,long fileHandle) {
		// retrieveTest(fileHandle);
		// byte[] b = outgoingMessage.getFile();
		// try {
			// File jfile = new File("local/" + id);
			// RandomAccessFile f = new RandomAccessFile(jfile,"rw");
			// f.seek(0);
			// f.skipBytes((int)f.length());
			// int length = b.length;
			// f.writeInt(length);
			// f.write(b);
		// }
		// catch (IOException e) {
			// e.printStackTrace();
		// }
	// }
	
	//For deleting files in the chunkServer map and in the folder
	public void deleteTest(long fileHandle) {
		String fileName = mapHandlePath.get(fileHandle);
		String newLocation = location + fileName;
		outgoingMessage.ImChunk();
		//Deletiing in local folder
		File f = new File(newLocation);
		if(f.exists()) {
			f.delete();
			//Setting up the outgoing message
			//outgoingMessage.clearMsg();
			outgoingMessage.setName("Delete file in TFS request complete");
		}
		else {
			outgoingMessage.setName("Errror: File not found. Delete file in TFS request incomplete");
		}
		
		//Deleting in key and value in hashmap
		mapHandleFile.remove(fileHandle);
		mapHandlePath.remove(fileHandle);
	}
}
	
	
//Look for HACK AND WEIRD AND WAIT if checking for parts still not sure
/**
	- Parse String for name of file in local system (fileName)
	- value of 'location' should also be in config file
	
Messages that chunkServer will receive
 - copyFile to TFS - DONE
 - copyFile from TFS - DONE
 - deleteFile - DONE
 - appendFile
 - Heartbeat
 - chunkServer to chunkServer

 Add print messages after incoming before outgoing

**/