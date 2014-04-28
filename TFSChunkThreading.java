import java.util.*;
import java.io.*;
import java.net.*;

public class TFSChunkThreading implements Runnable{
	
	TFSChunkServer mPrime;
	boolean initialized;
	
	private List<TFSMessage> outgoingMessages; //This message is used to convey information to other entities in the system
	private List<TFSMessage> incomingMessages; // This Queue will store any incoming messages
	
	//Where in local are we storing all files - default location - better if in config file
	String location;
	//Relating handles and files
	//Map<Long, byte[]> mapHandleFile = new HashMap<Long, byte[]>();
	Map<Long, byte[]> mapHandleFile = Collections.synchronizedMap(new HashMap<Long, byte[]>());
	//Relating handles and file names in local folder
	//Map<Long, String> mapHandlePath = new HashMap<Long, String>();
	Map<Long, String> mapHandlePath = Collections.synchronizedMap(new HashMap<Long, String>());
	
	
	//Stores own IP to attach to outgoing messages
	String myIP;
	
	//read from config_chunk to initialize port and connections
	String hostName;
	int portNumber = 8000; //Default unless in stated in the config fle
	
	/**Constructor for TFSChunkThreading
	* sets up all config files
	*/
	public TFSChunkThreading() {
		setUpChunk();
		
		mPrime = new TFSChunkServer(this);
		initialized = false;
		
		Thread thread2 = new Thread(mPrime);
		thread2.start();
	}
	
	/**Setting up the chunk - setUpChunk
	* reads from config files
	* initiates handshake
	*/
	private void setUpChunk() {
		/** All initialization for the chunk should be done here **/
		System.out.println("Welcome to TFS ChunkServer");
		System.out.println("Loading configuration files");
		
		
		//Retrieve configuration data from config_chunk.txt
		try {
			Scanner configFile = new Scanner(new File("config.txt"));
			while (configFile.hasNext()){
				String input = configFile.next();
				if (input.equals("MASTER"))
					hostName = configFile.next();
				//else if (input.equals("PORT"))
				//	portNumber = Integer.valueOf(configFile.next());
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
			System.out.println("ChunkServer: my IP Address is " + myIP);
			TFSMessage handshakeMessage = new TFSMessage(myIP,TFSMessage.Type.CHUNK);
			handshakeMessage.setMessageType(TFSMessage.mType.HANDSHAKE);
			handshakeMessage.setDestination(hostName);
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
		
		outgoingMessages = Collections.synchronizedList(new ArrayList<TFSMessage>());
		incomingMessages = Collections.synchronizedList(new ArrayList<TFSMessage>());
		
		System.out.println("Initialization of ChunkServer is complete");
	}
	
	/**getOutgoingMessage
	* returns a list of outgoing message
	*/
	public List<TFSMessage> getOutgoingMessages() {
		return outgoingMessages;
	}
	
	/**addOutgoingMessage
	* @param Message
	* adds message to be sent
	*/
	public void addOutgoingMessage(TFSMessage m) {
		synchronized(outgoingMessages) {
			outgoingMessages.add(m);
		}
	}
	
	/**infinite while loop
	*/
	public void run() {
		while(true) {
			scheduler();
		}
	}
	
	/**getLocation
	* returns the local folder directory
	* for chunkServer
	*/
	public String getLocation() {
		return location;
	}
	
	/**getPathMap
	* returns the Handle to file Name map
	*/
	public Map getPathMap() {
		return mapHandlePath;
	}
	
	/**getFileMap
	* returns the Handle to byteArray map
	*/
	public Map getFileMap() {
		return mapHandleFile;
	}
	
	/**addFileToMap
	* @param fileHandle(long), byteArray
	* adds to map
	*/
	public void addFileToMap(long fileHandle, byte[] b) {
		mapHandleFile.put(fileHandle, b);
	}
	
	/**addPathToMap
	* @param fileHandle(long), fileName(String)
	* adds to map
	*/
	public void addPathToMap(long fileHandle, String fileName) {
		mapHandlePath.put(fileHandle,fileName);
	}
	
	/**deletePathInMap
	* @param fileHandle(long)
	* removes key and value in map
	*/
	public void deletePathInMap(long fileHandle) {
		mapHandlePath.remove(fileHandle);
	}
	
	/**deleteFileInMap
	* @param fileHadle(long)
	*removes key and value in map
	*/
	public void deleteFileInMap(long fileHandle) {
		mapHandleFile.remove(fileHandle);
	}
	
	/**scheduler - checks if there is anything the
	* chunkserver can do
	*/
	private void scheduler() {
		try {
			if (!outgoingMessages.isEmpty()) {
				System.out.println("calling");
				sendTraffic(outgoingMessages.remove(0));
			}
			else {
				incomingMessages = listenForTraffic(incomingMessages); //update incomingMessages as required
				if (!incomingMessages.isEmpty()){ //If we have messages that need to be processed
					System.out.println("Parsing message");
			 		parseMessage(incomingMessages.remove(0)); // identify what needs to be done based on the parameters of the first message, and respond
				}
			}
			try { Thread.sleep(100); } catch(InterruptedException e) {}
		} catch (ClassNotFoundException e){
			 System.out.println("error");
			 while (incomingMessages.isEmpty()){//REMOVE
				 try {
					 incomingMessages = listenForTraffic(incomingMessages); //update incomingMessages as required
					 	if (!incomingMessages.isEmpty()){ //If we have messages that need to be processed
					 		parseMessage(incomingMessages.remove(0)); // identify what needs to be done based on the parameters of the first message, and respond
					 	}
				 } catch (ClassNotFoundException e2){
					 System.out.println("error");
				 }
			 }
		}
	}
	
	/**sendTraffic
	* @param Message
	* method to send message
	*/
	private void sendTraffic(TFSMessage current){
		try (
            Socket messageSocket = new Socket(current.getDestination(), portNumber);
            ObjectOutputStream out =
                new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			current.setSource(myIP);
			current.sendMessage(out);
        } catch (UnknownHostException e) {
            System.err.println("Error: Don't know about host " + current.getDestination());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: Couldn't get I/O for the connection to " + current.getDestination());
            System.exit(1);
        } 
	}

	/**clears all elements of the message
	* @param Message
	* not really used
	*/
	private TFSMessage resetMessage(TFSMessage m){
		//change all parameters besides messageSource and sourceType to null types 
		return m;
	}
	
	/**Waiting if there are any
	* possible incoming messages
	* @param list of incoming messages
	*/
	private List<TFSMessage> listenForTraffic(List<TFSMessage> q) throws ClassNotFoundException{
		try (
			ServerSocket serverSocket =
                new ServerSocket(portNumber);
            Socket clientSocket = serverSocket.accept();     
            ObjectOutputStream out =
                new ObjectOutputStream(clientSocket.getOutputStream()); //To send messages, probably not necessary here
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()); //Receive messages from the client
        ) {
			try { Thread.sleep(1000); } catch(InterruptedException e) {}
			System.out.println("listening");
			TFSMessage incomingMessage = new TFSMessage(); //create a new MessageObject, I think we should have the constructor set everything to null when it's initialized
			while(incomingMessage.getMessageType() == TFSMessage.mType.NONE){
				incomingMessage.receiveMessage(in); //call readObject 
			}
			if (incomingMessage.getMessageType() != TFSMessage.mType.NONE){ //if we received data
				System.out.println("Received a message");
				q.add(incomingMessage);
			} 
			/*
			//Might make more sense to have an outgoingMessages Queue, and to send the Outgoing message with the proper flag set right after you read
			TFSMessage current = outgoingMessages.remove(0);
			if (current.getMessageType() != TFSMessage.mType.NONE)
				current.sendMessage(out);
			else 
				outgoingMessages.add(current);
			*/
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port " + portNumber + " or listening for a connection");
            e.printStackTrace();
        } catch (ClassNotFoundException e){
		System.out.println("error");
        }
        return q;
	}
	
	/** parse the message
	* @param Message
	*/
	private void parseMessage(TFSMessage m){
		//check the parameters of m, figure out the corresponding method to call for that
		//those methods should finish by sending out the message and resetting the outgoingMessage 
		m.setDestination(m.getSource());
		TFSMessage.mType type = m.getMessageType();
		mPrime.addMessage(m);
	}
	
	/**Main**/
	public static void main(String[] args) {
		TFSChunkThreading cThread = new TFSChunkThreading();
		Thread thread = new Thread(cThread);
		thread.start();
	}
}
