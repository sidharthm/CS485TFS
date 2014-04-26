import java.util.*;
import java.io.*;
import java.net.*;

public class TFSChunkServer {

	//Where in local are we storing all files - default location - better if in config file
	String location;
	//Relating handles and files
	Map<Long, byte[]> mapHandleFile = new HashMap<Long, byte[]>();
	//Relating handles and file names in local folder
	Map<Long, String> mapHandlePath = new HashMap<Long, String>();
	//Stores own IP to attach to outgoing messages
	String myIP;
	//64-bit handles for files
	long nextAvailableHandle = 0;
	//IDs for chunkServer to differentiate
	int chunkServerID = 0;
	static int nextAvailableChunkServerID = 0;
	
	//HACK: BOOLEAN FOR NOW
	//boolean Error = false;
	//LATER: Should read from config_chunk to initialize port and connections
	String hostName;
	int portNumber;
	
	TFSMessage outgoingMessage;
	TFSMessage incomingMessage;
	
	public TFSChunkServer() {
		chunkServerID = nextAvailableChunkServerID;
		//location = fileLocation;
		nextAvailableChunkServerID++;
		setUpChunk();
	}
	
	private void setUpChunk() {
		/** All initialization for the chunk should be done here **/
		System.out.println("Welcome to TFS ChunkServer");
		System.out.println("Loading configuration files");
		
		
		//Retrieve configuration data from config_chunk.txt
		try {
			Scanner configFile = new Scanner(new File("chunk_config.txt"));
			while (configFile.hasNext()){
				String input = configFile.next();
				if (input.equals("MASTER"))
					hostName = configFile.next();
				else if (input.equals("PORT"))
					portNumber = Integer.valueOf(configFile.next());
				else if(input.equals("LOCATION"))
					location = configFile.next();
			}
			configFile.close();
		} catch (FileNotFoundException e){
			System.err.println("Error: Configuration file not found");
			System.exit(1);
		}
		
		System.out.println("Master: " + hostName);
		System.out.println("Port: " + portNumber);
		System.out.println("Saving in location: " + location);
		
		//Initializing communication with Server through handshake
		try (
            Socket messageSocket =
                new Socket(hostName, portNumber);
			ObjectOutputStream out =
                new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket   
        ) {
			//Converting chunk IP to string
			myIP = messageSocket.getLocalAddress().toString();
			myIP = myIP.substring(1);
			System.out.println("ChunkServer " + chunkServerID + ": my IP Address is " + myIP);
			TFSMessage handshakeMessage = new TFSMessage(myIP,TFSMessage.Type.CHUNK);
			handshakeMessage.setMessageType(TFSMessage.mType.HANDSHAKE);
			handshakeMessage.sendMessage(out);
			messageSocket.close();//Done, so let's close this
			//This is to basically store in the hashmap whatever files are already in the folder
			//retrieveFiles();
		
		} catch (UnknownHostException e) {
            System.err.println("Error: Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: Couldn't get I/O for the connection to " + hostName);
            System.exit(1);
        }
		
		outgoingMessage = new TFSMessage(myIP,TFSMessage.Type.CHUNK);
		incomingMessage = new TFSMessage();
		
		System.out.println("Initialization of ChunkServer " + chunkServerID + " is complete");
	}
	
	public int getID() {
		return chunkServerID;
	}
	
	public int numOfFile() {
		return mapHandleFile.size();
	}
	
	private void Run() {
		while (true){
			
			//Wait for receive messages
			listenForMessages();
			
			/**
			if (content != null){ //if we received data
				//HACK: STILL NEED A SWITCH STATEMENT HERE
				
				//chunkserver.storeTest(incomingMessage.getFile());
				
				int receivedHandle = 0;
				//Error = chunkserver.retrieveTest(receivedHandle);
				//chunkserver.retrieveTest(receivedHandle);
			
				//chunkserver.deleteTest(receivedHandle);
				
				chunkserver.send(out);				
			}
			**/
		}
	}
	
	private void listenForMessages() {
		try (
            ServerSocket serverSocket =
                new ServerSocket(portNumber);
            Socket inmessageSocket = serverSocket.accept();     
			//Receive messages
            ObjectInputStream in = new ObjectInputStream(inmessageSocket.getInputStream()); 
        ) {
			//we could use a timer to keep it from hanging indefintely
			System.out.println("ChunkServer " + getID() + ": Waiting for connections...");
			while(incomingMessage.getMessageType() == TFSMessage.mType.NONE){
				incomingMessage.receiveMessage(in); //call readObject 
			}
			//Print out the received request from sender
			System.out.println("ChunkServer " + getID() + ": Receive request of " + incomingMessage.getMessageType().toString() + " from " +  incomingMessage.getSourceType().toString() + " " + incomingMessage.getSource());
			
			serverSocket.close();
		} catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e){
			System.out.println("Class error found when trying to listen to port " + portNumber);
			System.out.println(e.getMessage());
		}
		parseMessage(incomingMessage);	
	}
	
	private void parseMessage(TFSMessage t) {
		//HACK
		long HACK = 1234;
		//Read the request and read paramaters appropriately and call the right action
		switch(t.getMessageType()) {
			//Two possibilities, talking with client and talking with master
			case CREATEFILE:
				if(t.getSourceType() == TFSMessage.Type.CLIENT)
					//I NEED FILEHANDLE (LONG) AND FILE (BYTE[])
					createFile(t.getSource(), HACK, t.getBytes());
				else if(t.getSourceType() == TFSMessage.Type.MASTER)
					//I NEED FILEHANDLE (LONG) AND FILENAME
					createFile(t.getSource(), HACK, t.getFileName());
				break;
			
			case DELETE:
				//I NEED FILEHANDLE (LONG)
				deleteFile(t.getSource(), HACK);
				break;
			
			//Consistent communication with master
			case HEARTBEAT:
				heartbeatResponse(t.getSource());
				break;
			
			case SEEK:
				//NOT SURE ABOUT PARAMETERS
				seekAndWrite();
				break;
			
			case SIZEDAPPEND:
				//NOT SURE ABOUT PARAMETERS
				sizedAppend();
				break;
			
			case APPEND:
				//NOT SURE ABOUT PARAMETERS
				append();
				break;
			
			//Allows client to receive a file from the TFS
			case READFILE:
				//I NEED FILEHANDLE (LONG)
				readFile(t.getSource(), HACK);
				break;
			
			case COUNTFILES:
				//I NEED FILEHANDLE - ALSO NEED SOME INT TO STORE THE COUNT INFO
				countFiles(t.getSource(), HACK);
				break;
			
			case CREATEREPLICA:
				//NOT SURE ABOUT PARAMETERS
				createReplica();
				
				break;
			
			//DON'T THINK CHUNKSERVER WOULD NEED THIS
			// case SUCCESS:
				// success();
				// break;
			
			// case ERROR:
				// error();
				// break;
			
			default:
				parseImpossible(t.getSource(),t.getMessageType().toString());
				System.out.println();
				break;
		}
	}

	private void parseImpossible(String sender, String msg) {
		System.out.println("ChunkServer " + getID() + ": CAN'T UNDERSTAND request " + msg + " from " + sender);
		
		//Setting up outgoing message
		//Sending message back to sender of request
		outgoingMessage.setDestination(sender);
		//Request is not completed
		outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
		send(sender);
	}
	
	//Create File request from Server to setup file handle and file name
	private void createFile(String sender, long fileHandle, String fileName) {		
		String newLocation = location + fileName;
		try{
			//Creating a new file
			File f = new File(newLocation);
			//File already exists
			if(f.exists()) {
				System.out.println("ChunkServer " + chunkServerID + ": Error, file already exists in local.");				
				//Setting up outgoing message
				//Request is not completed
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			else {
				System.out.println("ChunkServer " + getID() + ": Creating file, " + fileName + ".");
				//Empty file
				FileOutputStream fileOutputStream = new FileOutputStream(newLocation);
				fileOutputStream.close();
				//Since adding file to local is successful need to add to map
				mapHandlePath.put(fileHandle,fileName);
				//nextAvailableHandle++;
				System.out.println("ChunkServer " + getID() + ": File, " + fileName + ", created.");
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
		send(sender);
	}
		
	//createFile request from client overwrites just created empty file from server
	private void createFile(String sender, long fileHandle, byte[] b) {		
		String fileName = mapHandlePath.get(fileHandle);
		String newLocation = location + fileName;
		try{
			//Creating a new file
			File f = new File(newLocation);
			//File already exists
			if(!f.exists()) {
				System.out.println("ChunkServer " + chunkServerID + ": Error, file not yet exist in local.");				
				//Setting up outgoing message
				//Request is not completed
				outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
			}
			else {
				System.out.println("ChunkServer " + getID() + ": Creating (Overwriting) file, " + fileName + ", for client.");
				//Opening local location and writing/adding file to it
				FileOutputStream fileOutputStream = new FileOutputStream(newLocation);
				fileOutputStream.write(b);
				fileOutputStream.close();
				//Since adding file to local is successful need to add to map
				mapHandleFile.put(fileHandle, b);
				//nextAvailableHandle++;
				System.out.println("ChunkServer " + getID() + ": File, " + fileName + ", created (overwrited) for client.");
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
		send(sender);
	}
	
	//For deleting files in the chunkServer map and in the folder
	private void deleteFile(String sender, long fileHandle) {
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
		mapHandleFile.remove(fileHandle);
		mapHandlePath.remove(fileHandle);
		send(sender);
	}	
	
	private void heartbeatResponse(String sender) {
		//Setting up outgoing message
		outgoingMessage.setMessageType(TFSMessage.mType.HEARTBEATRESPONSE);
		outgoingMessage.setDestination(sender);
		send(sender);
	}
	
	private void readFile(String sender, long fileHandle) {
		//Since all files in the folder were retrieved in start of chunkserver and all files added are in the map as well, there is no need to read for local files to send the byte array to client
		byte[] b;
		b = mapHandleFile.get(fileHandle);
		if(b != null) {
			//Setting up the outgoing message
			outgoingMessage.setBytes(b);
			outgoingMessage.setMessageType(TFSMessage.mType.SUCCESS);
		}
		else {
			System.out.println("ChunkServer " + chunkServerID + ": Error, chunkHandle does not exist.");
			//Setting up outgoing message
			//Request is not completed
			outgoingMessage.setMessageType(TFSMessage.mType.ERROR);
		}
		outgoingMessage.setDestination(sender);
		send(sender);
	}
	
	//NOT YET IMPLEMENTED
	private void seekAndWrite() {
	
	}
	
	//NOT YET IMPLEMENTED
	private void sizedAppend() {
	
	}
	
	//NOT YET IMPLEMENTED
	private void append() {
	
	}
	
	private void countFiles(String sender, long fileHandle) {
		int count = 0;
		String fileName = mapHandlePath.get(fileHandle);
		String newLocation = location + fileName;
		try{
			//Creating a new file
			File f = new File(newLocation);
			//File already exists
			if(!f.exists()) {
				System.out.println("ChunkServer " + chunkServerID + ": Error, file not yet exist in local.");				
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
				//HACK: I DON'T KNOW WHERE TO PUT THE COUNT
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		outgoingMessage.setDestination(sender);
		send(sender);
	}

	//NOT YET IMPLEMENTED
	private void createReplica() {
	
	}
	
	/**private void success() {
		//HACK
		//HONESTLY, I DON'T THINK CHUNKSERVER WOULD NEED TO KNOW SUCCESS OR SOMETHING
		//WHEN WILL THIS HAPPEN?
	}
	
	private void error() {
		//HACK
		//HONESTLY, I DON'T THINK CHUNKSERVER WOULD NEED TO KNOW SUCCESS OR SOMETHING
		//WHEN WILL THIS HAPPEN?
	}**/
	
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
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {

		TFSChunkServer chunkserver = new TFSChunkServer();
		chunkserver.Run();
		
		/**
        // if (args.length != 1) {
            // System.err.println("Usage: java EchoServer <port number>");
            // System.exit(1);
        // }
        
        //int portNumber = Integer.parseInt(args[0]);
		int portNumber = 4444;
		
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
		**/
	}
}
	
	
//Look for HACK AND WEIRD AND WAIT if checking for parts still not sure
/**
	
//NOT YET IMPLEMENTED
- seekAndWrite
- sizedAppend()
- append()
- createReplica
 
 chunkServer ID might need to be randomize

**/