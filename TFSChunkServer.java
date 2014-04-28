import java.util.*;
import java.io.*;
import java.net.*;

public class TFSChunkServer implements Runnable{

	//Where in local are we storing all files - default location - better if in config file
	String location;
	
	TFSChunkThreading chunkThread;
	private List<TFSMessage> incomingMessages; // This Queue will store any incoming messages
	private TFSMessage outgoingMessage; 
	private int nextAvailableHandle = 0;
	
	/**Constructor for TFSChunkServer
	* @param instance of TFSChunkThreading
	*/
	public TFSChunkServer(TFSChunkThreading c) {
		/*Set up all messages with the appropriate initialization*/
		incomingMessages = Collections.synchronizedList(new ArrayList<TFSMessage>());
		outgoingMessage = new TFSMessage();
		chunkThread = c;
		location = c.getLocation();
	}
	
	// public int getID() {
		// return chunkServerID;
	// }
	
	// public int numOfFile() {
		// return mapHandleFile.size();
	// }
	
	/**Infinite while loop
	* to check if there is anything to do
	*/
	public void run() {
		while (true) {
			scheduler();
		}
	}
	
	/**Scheduler
	* similar to agent scheduler
	* just keeps checking if there
	* is anything to do
	**/
	public void scheduler() {
		if (incomingMessages.size() > 0) {
			TFSMessage t;
			synchronized(incomingMessages) {
				t = incomingMessages.remove(0);
			}
			TFSMessage.mType type = t.getMessageType();
			switch(type) {
			//Two possibilities, talking with client and talking with master
			case CREATEFILE:
				if(t.getSourceType() == TFSMessage.Type.CLIENT)
					//I NEED FILEHANDLE (LONG) AND FILE (BYTE[])
					createFile(t.getSource(), t.getFileID(), t.getBytes());
				else if(t.getSourceType() == TFSMessage.Type.MASTER)
					//I NEED FILEHANDLE (LONG) AND FILENAME
					createFile(t.getSource(), t.getFileID(), String.valueOf(t.getFileID()));
				break;
			
			case DELETE:
				deleteFile(t.getSource(), t.getFileID());
				break;
			
			//Consistent communication with master
			case HEARTBEAT:
				heartbeatResponse(t.getSource());
				break;
			
			case SEEK:
				seekAndWrite(t.getSource(), t.getOffset(), t.getFileID(), t.getBytes());
				break;
			
			case SIZEDAPPEND:
				sizedAppend(t.getSource(), t.getFileID(), t.getBytes());
				break;
			
			case APPEND:
				append(t.getSource(), t.getFileID(), t.getBytes());
				break;
			
			//Allows client to receive a file from the TFS
			case READFILE:
				readFile(t.getSource(), t.getFileID());
				break;
			
			case COUNTFILES:
				//I NEED FILEHANDLE - ALSO NEED SOME INT TO STORE THE COUNT INFO
				countFiles(t.getSource(), t.getFileID());
				break;
			
			//DON'T THINK NEED THIS CAUSE MASTER SENDS CREATEFILE FOR REPLICAS TOO.
			//case CREATEREPLICA:
				//NOT SURE ABOUT PARAMETERS
				//createReplica();
				//break;
			
			//DON'T THINK CHUNKSERVER WOULD NEED THIS
			// case SUCCESS:
				// success();
				// break;
			
			// case ERROR:
				// error();
				// break;
			
			default:
				// parseImpossible(t.getSource(),t.getMessageType().toString());
				// System.out.println();
				break;
			}		
		}
		try { Thread.sleep(10); } catch(InterruptedException e) {}
	}
/**
	private void parseImpossible(String sender, String msg) {
		System.out.println("ChunkServer " + getID() + ": CAN'T UNDERSTAND request " + msg + " from " + sender);
		
		//Setting up outgoing message
		//Sending message back to sender of request
		outgoingMessage.setDestination(sender);
		//Request is not completed
		outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
		send(outgoingMessage);
	}
**/

	/**For creating files
	* @param sender (String), fileHandle (long), fileName (String)
	* for creating file request from server
	*/
	//Create File request from Server to setup file handle and file name
	private void createFile(String sender, long fileHandle, String fileName) {	
		String newLocation = location + fileName;
		try{
			//Creating a new file
			File f = new File(newLocation);
			//File already exists
			if(f.exists()) {
				System.out.println("ChunkServer: Error, file already exists in local.");				
				//Setting up outgoing message
				//Request is not completed
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			else {
				System.out.println("ChunkServer: Creating file, " + fileName + ".");
				//Empty file
				FileOutputStream fileOutputStream = new FileOutputStream(newLocation);
				fileOutputStream.close();
				//Since adding file to local is successful need to add to map
				synchronized(chunkThread.getPathMap()) {
					chunkThread.addPathToMap(fileHandle, fileName);
				}
				//mapHandlePath.put(fileHandle,fileName);
				//nextAvailableHandle++;
				System.out.println("ChunkServer: File, " + fileName + ", created.");
				//Setting up outgoing message
				//Request complete
				outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
			}
		}
		catch (IOException e) {
			//Setting up outgoing message
			//Request is not completed
			outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			e.printStackTrace();
		}
		//Sending message back to sender of request
		outgoingMessage.setDestination(sender);
		send(outgoingMessage);
	}
	
	/**For creating files 
	* @param sender(String), fileHandle(long), byteArray
	* to overwrite empty byte array from master message
	* coming from client
	*/	
	//createFile request from client overwrites just created empty file from server
	private void createFile(String sender, long fileHandle, byte[] b) {
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			String fileName = mapHandlePath.get(fileHandle);
			String newLocation = location + fileName;
			try{
				//Creating a new file
				File f = new File(newLocation);
				//File already exists
				if(!f.exists()) {
					System.out.println("ChunkServer: Error, file not yet exist in local.");				
					//Setting up outgoing message
					//Request is not completed
					outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
				}
				else {
					System.out.println("ChunkServer: Creating (Overwriting) file, " + fileName + ", for client.");
					//Opening local location and writing/adding file to it
					FileOutputStream fileOutputStream = new FileOutputStream(newLocation);
					fileOutputStream.write(b);
					fileOutputStream.close();
					//Since adding file to local is successful need to add to map
					chunkThread.addFileToMap(fileHandle, b);
					//mapHandleFile.put(fileHandle, b);
					//nextAvailableHandle++;
					System.out.println("ChunkServer: File, " + fileName + ", created (overwrited) for client.");
					//Setting up outgoing message
					//Request complete
					outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
				}
			}
			catch (IOException e) {
				//Setting up outgoing message
				//Request is not completed
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
				e.printStackTrace();
			}
			//Sending message back to sender of request
			outgoingMessage.setDestination(sender);
			send(outgoingMessage);
		}}
	}
	
	/** For deleting files
	* @param sender(String), fileHandle(long)
	* deletes file from all maps and memory
	*/
	//For deleting files in the chunkServer map and in the folder
	private void deleteFile(String sender, long fileHandle) {
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			String fileName = mapHandlePath.get(fileHandle);
			String newLocation = location + fileName;
			//outgoingMessage.ImChunk();
			//Deletiing in local folder
			File f = new File(newLocation);
			if(f.exists()) {
				f.delete();
				//Setting up the outgoing message
				outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
			}
			else {
				//Setting up the outgoing message
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			//Deleting in key and value in hashmap
			chunkThread.deleteFileInMap(fileHandle);
			chunkThread.deletePathInMap(fileHandle);
			send(outgoingMessage);
		}}
	}	
	
	/** Heartbeat response to master
	* @param sender(String) - master IP
	*/
	private void heartbeatResponse(String sender) {
		//Setting up outgoing message
		outgoingMessage.setMessageType(TFSMessage.mType.HEARTBEATRESPONSE);
		outgoingMessage.setDestination(sender);
		send(outgoingMessage);
	}
	
	/** readFile request from the client
	@param sender(String) - client, fileHandle(long)
	*/
	private void readFile(String sender, long fileHandle) {
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			//Since all files in the folder were retrieved in start of chunkserver and all files added are in the map as well, there is no need to read for local files to send the byte array to client
			byte[] b;
			b = mapHandleFile.get(fileHandle);
			if(b != null) {
				//Setting up the outgoing message
				outgoingMessage.setBytes(b);
				outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
			}
			else {
				System.out.println("ChunkServer: Error, chunkHandle does not exist.");
				//Setting up outgoing message
				//Request is not completed
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			outgoingMessage.setDestination(sender);
			send(outgoingMessage);
		}}
	}
	
	/**For atomic append - seekAndWrite
	*@param sender(String), offset(int), fileHandle(long), byteArray
	*very similar implementation to append just inserted in the middle
	*/
	private void seekAndWrite(String sender, int offset, long fileHandle, byte[] b) {
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			String newLocation = location + String.valueOf(fileHandle);	
			try {
				File jfile = new File(newLocation);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				f.seek((long)offset);
				f.write(b);
				byte[] new_b = new byte[(int)f.length()];
				f.close();
				chunkThread.addFileToMap(fileHandle, new_b);
				outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
			}
			catch (IOException e) {
				e.printStackTrace();
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			outgoingMessage.setDestination(sender);
			send(outgoingMessage);
		}}
	}
	
	/**sizedAppend - appending to a file including the size of the added file
	* @param sender(String), fileHandle(long), byteArray
	*/
	//Appending to a file with the initial 4 bit size
	private void sizedAppend(String sender, long fileHandle, byte[] b) {
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			String newLocation = location + String.valueOf(fileHandle);	
			try {
				File jfile = new File(newLocation);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				f.seek(0);
				f.skipBytes((int)f.length());
				int length = b.length;
				f.writeInt(length);
				f.write(b);
				//Copying the new byte array to the map, no need to update path map since filename stays the same
				byte[] new_b = new byte[(int)f.length()];
				f.close();
				chunkThread.addFileToMap(fileHandle, new_b);
				outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
			}
			catch (IOException e) {
				e.printStackTrace();
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			outgoingMessage.setDestination(sender);
			send(outgoingMessage);
		}}
	}
	
	/*Simple append
	* @param sender(String), fileHandle(long), byteArray
	* adds a file to the end of a specific file
	*/
	//Simple append
	private void append(String sender, long fileHandle, byte[] b) {
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			String newLocation = location + String.valueOf(fileHandle);	
			try {
				File jfile = new File(newLocation);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				f.seek(0);
				f.skipBytes((int)f.length());
				f.write(b);
				//Copying the new byte array to the map, no need to update path map since filename stays the same
				byte[] new_b = new byte[(int)f.length()];
				f.close();
				chunkThread.addFileToMap(fileHandle, new_b);
				outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
			}
			catch (IOException e) {
				e.printStackTrace();
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			outgoingMessage.setDestination(sender);
			send(outgoingMessage);
		}}
	}
	
	/**Sends the number of files in a chunk
	* @param sender(String), fileHandle(long)
	*/
	//Sends the number of files in a chunk
	private void countFiles(String sender, long fileHandle) {
		int count = 0;
		synchronized(chunkThread.getFileMap()) { synchronized(chunkThread.getPathMap()) {
			Map<Long, byte[]> mapHandleFile = chunkThread.getFileMap();
			Map<Long, String> mapHandlePath = chunkThread.getPathMap();
			String fileName = mapHandlePath.get(fileHandle);
			String newLocation = location + fileName;
			try{
				//Creating a new file
				File f = new File(newLocation);
				//File already exists
				if(!f.exists()) {
					System.out.println("ChunkServer: Error, file not yet exist in local.");				
					//Setting up outgoing message
					//Request is not completed
					outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
				}
				else {
					RandomAccessFile rf = new RandomAccessFile(f,"rw");
					long delta = 0;
					while (delta < rf.length()) {
						rf.seek(delta);
						int size = rf.readInt();
						delta = delta + 4 + (long)size;
						count++;
					}
					//Setting up the outgoing message
					outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
					outgoingMessage.setNumFiles(count);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			outgoingMessage.setDestination(sender);
			send(outgoingMessage);
		}}
	}

/**
	//Not really needed because replicas are createFile messages
	private void createReplica() {
	
	}
	private void success() {
		//HONESTLY, I DON'T THINK CHUNKSERVER WOULD NEED TO KNOW SUCCESS OR SOMETHING
	}
	private void error() {
		//HONESTLY, I DON'T THINK CHUNKSERVER WOULD NEED TO KNOW SUCCESS OR SOMETHING
	}

	private void send(String senderIP) {
		try (
            Socket messageSocket = new Socket(senderIP, portNumber);
            ObjectOutputStream out = new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			//Print out the received request from sender
			System.out.println("ChunkServer " + getID() + ": Sending message " + outgoingMessage.getMessageType().toString() + " to " + outgoingMessage.getSource());
			outgoingMessage.sendMessage(out); //send the message to the server
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
			//Error = true;
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName);
			//Error = true;
        }
	}	
**/

	/**adding to the messages incoming - not really used
	* @param Message
	*/
	public void addMessage(TFSMessage m) {
		incomingMessages.add(m);
	}
	
	/**adds messages to the outgoing queue in TFSChunkThreading - send
	* @param Message
	*/
	public void send(TFSMessage m) {
		chunkThread.addOutgoingMessage(m);
	}

	/**Retrieving files in local directory(folder)
	* since working with persistent data, any file saved by
	* client in TFS is stored in local memory of chunkServer
	*/
	//Retrieve Files
	private void retrieveFiles() {
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
					
					//mapHandleFile.put(nextAvailableHandle, b);
					//mapHandlePath.put(nextAvailableHandle, file.getName());
					chunkThread.addFileToMap(nextAvailableHandle, b);
					chunkThread.addPathToMap(nextAvailableHandle, file.getName());
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
}