import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class TFSClient{
	TFSMaster master;
	String input=null;
	int dir;
	int sub;
	String[] commands=null;
	String location="C: ";
	String chunk_ID=null;
	String chunk_location=null;
	Timer timer = new Timer();
	int testone;
	List<String> directories=new ArrayList<String>();
	ArrayList<String[]> paths = new ArrayList<String[]>();
	boolean done=true;
	boolean Error = false;
	String backslash = "\\";
	
	String hostName; //Load this from a file, it stores the master server's IP
	String myName; //This stores the server's own IP so that it can attach that information to messages
	int portNumber = 8000; //Change this if you want to communicate on a different port
	Socket clientMessageSocket; //This is the socket the client uses to contact the master
	
	TFSMessage outgoingMessage;
	TFSMessage incomingMessage;
	
	public TFSClient(){
		setUpClient();
	}
	/**
	 * Initial setup of the client
	 */
	public void setUpClient(){
		/*This first block gets the master's IP address and loads it into the client*/
		System.out.println("Welcome to TFS Client");
		System.out.println("Loading configuration files");
		try {
			Scanner inFile = new Scanner(new File("config.txt"));
			while (inFile.hasNext()){
				String input = inFile.next();
				if (input.equals("MASTER"))
					hostName = inFile.next();
			}
		} catch (FileNotFoundException e){
			System.err.println("Error: Configuration file not found");
			System.exit(1);
		}
		
		/*
		* The next block sets up the Socket for the first time, allowing the client to communicate
		* with the Master. 
		*/
		System.out.print("Connecting to TFS Master Server from ");
		try (
            Socket messageSocket = new Socket(hostName, portNumber);
            ObjectOutputStream out =
                new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			myName = messageSocket.getLocalAddress().toString(); //Convert client's IP address to a string
			myName = myName.substring(1); //Get rid of the first slash
			System.out.println(myName); //Print-out to confirm contents
			TFSMessage handshakeMessage = new TFSMessage(myName,TFSMessage.Type.CLIENT);
			handshakeMessage.setMessageType(TFSMessage.mType.HANDSHAKE);
			handshakeMessage.sendMessage(out);
			messageSocket.close();//Done, so let's close this
        } catch (UnknownHostException e) {
            System.err.println("Error: Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: Couldn't get I/O for the connection to " + hostName);
            System.exit(1);
        } 
		
		//Here, we initialize the TFSMessage object that the Client will send out with the appropriate information about the client machine
		outgoingMessage = new TFSMessage(myName,TFSMessage.Type.CLIENT);
		incomingMessage = new TFSMessage();
		
		System.out.println("Initialization of Client complete");
	}
	/**
	 * Sets the master and calls to print out console
	 */
	public void setMaster(TFSMaster m) {
		master = m;
		console();
	}
	/**
	 * Prints out console and waits for input
	 */
	public void console(){
		/*
		if(!paths.isEmpty()){
			makeDirectory(paths.remove(0));
		}
		*/
		System.out.print(location);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try{
			input=br.readLine();
		}catch (IOException ioe){
			System.out.println("IO Error");
		}
		actions();
		
	}
	/**
	 * Reads input and does called command
	 */
	public boolean actions(){
		commands=input.split(" ");
		
		if(commands[0].equalsIgnoreCase("mkdir")){
			if(commands.length == 2) {
				makeDirectory();
			}
		}
		else if (commands[0].equalsIgnoreCase("rm")){
			if(commands.length == 2)
				remove();
		}
		else if (commands[0].equalsIgnoreCase("seek")){
			if(commands.length == 4)
				seekFile();
		}
		else if (commands[0].equalsIgnoreCase("append")){
			if(commands.length == 3)
				appendFile();
		}
		else if(commands[0].equalsIgnoreCase("create")){
			createFile();
			done=false;
		}
		else if(commands[0].equalsIgnoreCase("Unit1")){
			if(commands.length == 3)
				unitOne();
		}
		else if(commands[0].equalsIgnoreCase("Unit2")){
			if(commands.length == 4)
				unitTwo();
		}
		else if(commands[0].equalsIgnoreCase("Unit3")){
			if(commands.length == 2)
				unitThree();
		}
		else if(commands[0].equalsIgnoreCase("Unit4")){
			if(commands.length == 3)
				unitFour();
		}
		else if(commands[0].equalsIgnoreCase("Unit5")){
			if(commands.length == 3)
				unitFive();
		}
		else if(commands[0].equalsIgnoreCase("Unit6")){
			if(commands.length == 3)
				unitSix();
		}
		else if(commands[0].equalsIgnoreCase("Unit7")){
			if(commands.length == 2)
				unitSeven();
		}
		else if(commands[0].equalsIgnoreCase("Unit8")){
			if(commands.length == 3)
				unitEight();
		}
		else if (commands[0].equals("?")){
			System.out.println("    Test1 <number of directories> - create directories");
			System.out.println("    Test2 <TFS directory> <number of fiels> - create files in directories and in its subdirectories");
			System.out.println("    Test3 <TFS directory> - delete files and directories in this directory");
			System.out.println("    Test4 <local file path> <TFS file path> - copy image in local to TFS");
			System.out.println("    Test5 <TFS file path> <local file path> - copy image from TFS to local");
			System.out.println("    Test6 <local file path> <TFS file path> - append local file to TFS file");
			System.out.println("    Test7 <TFS file path> - count files in TSF file");
		}
		
		
		else if(commands[0].equals("print")) {	//Debugging purposes
			//master.printTree(master.getRoot());//HERE
			outgoingMessage.setMessageType(TFSMessage.mType.PRINT);
		}
		else if (commands[0].equals("exit"))
			return false;
		else 
			System.out.println("Invalid command");
		sendMessageToMaster();
		try{
		listenForResponse();
		}
		catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e){
			System.out.println("Class error found when trying to listen to port " + portNumber);
			System.out.println(e.getMessage());
		}
		//if (flag for needing a server response) listenForResponse()
		console();
		return true;
	}
	/**
	 * Runs the first test: Creates hierarchical directory structure with a specified fanout
	 * Parameters from user input: two integers for amount of dicectories and subdirectories
	 */
	private void unitOne(){
		int k=1;
		int mult=1;
		boolean first=false;
		dir=Integer.parseInt(commands[1]);
		sub=Integer.parseInt(commands[2]);
		for (int i = 1; i <= dir; i++) {
			ArrayList<String> strings = new ArrayList<String>();
			int j = i;
			while (j > 1) {
				strings.add(""+j);
				j =j- sub;
			}
			strings.add(""+1);
			Collections.reverse(strings);
			String[] path = strings.toArray(new String[strings.size()]);
			for (int l = 0; l < path.length; l++) {
				System.out.println(path[l]);
			}
			makeDirectory(path);
			sendMessageToMaster();
			try { Thread.sleep(1000); } catch(InterruptedException e) {}
		}
	}
	/**
	 * Runs the second test: create N files in a directory and its subdirectories until the leaf subdirectories
	 * Parameters from user input: a string (path) and the amount of files (integer)
	 */
	private void unitTwo(){
		String[] d=commands[1].split("/");
		String[] path = new String[d.length-1];
		for (int i = 0; i < path.length; i++) {
			path[i] = d[i];
		}
		int num = Integer.parseInt(commands[2]);
		int replicas = Integer.parseInt(commands[3]);
		System.out.println("Client: Sending createFiles request to Master.");
		//master.recursiveCreateFileInitial(d, true, num);//HERE
		outgoingMessage.setMessageType(TFSMessage.mType.RECURSIVECREATE);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
		outgoingMessage.setFileNum(num);
		outgoingMessage.setReplicaNum(replicas);
	}
	/**
	 * Runs the third test: Delete a hierarchical directory structure including the files in those directories.
	 * Parameter from user input: a string (path)
	 */
	private void unitThree(){
		String[] delete=commands[1].split("/");
		String[]path = new String[delete.length-1];
		for (int i = 0; i < delete.length-1; i++) {
			path[i] = delete[i];
		}
		System.out.println("Client: Sending deleteDirectories request to Master.");
		//master.recursiveDelete(delete,true);
		outgoingMessage.setMessageType(TFSMessage.mType.DELETE);
		outgoingMessage.setPath(path);
		//outgoingMessage.setFileName(delete[delete.length-1]);
	}
	/**
	 * Runs the fourth test: Store a file on the local machine in a target TFS file specified by its path.
	 * Paramater from user input: string (local path), the file (sent in bytes), and number of replicas (integer) 
	 */
	private void unitFour(){
		String[] path = commands[2].split("/");
		String[] p = new String[path.length-1];
		for (int i = 0; i < p.length; i++) {
			p[i] = path[i];
		}
		//if (master.createFileNoID(p,path[path.length-1],true,true)) {//HERE
			File file = new File(commands[1]);
			try {
				RandomAccessFile f = new RandomAccessFile(file, "r");
				byte[] b = new byte[(int)f.length()];
				f.read(b);
				System.out.println("Client: Sending copyFile request from local to TFS request to Master.");
				outgoingMessage.setMessageType(TFSMessage.mType.APPEND);
				outgoingMessage.setPath(p);
				outgoingMessage.setBytes(b);
				//master.appendToFileNoSize(path,b);//HERE
			}
			catch (IOException e) {
				System.out.println("Client: Error, can't locate local file path.");
				e.printStackTrace();
			}
		//}
		//else
		//	System.out.println(" File exists in TFS.");
	}
	/**
	 *Runs the fifth test: Read the content of a TFS file and store it on the specified file on the local machine.
	 *Parameter from user input: string (local path) and file (sent in bytes) 
	 */
	private void unitFive(){
		String[] path = commands[1].split("/");
		String[] p = new String[path.length-1];
		for (int i = 0; i < p.length; i++) {
			p[i] = path[i];
		}
		System.out.println("Client: Sending getFile request from TFS to local to Master.");
		//byte[] b = master.readFileNoSize(d);//HERE
		outgoingMessage.setMessageType(TFSMessage.mType.READFILE);
		outgoingMessage.setPath(path);
		/*if(!Error) {
			try {
				File f = new File(commands[2]);
				if(f.exists()) {
					System.out.println("Client: Error, file already exists in local.");
				}
				else {
					FileOutputStream fileOuputStream = new FileOutputStream(commands[2]); 
					fileOuputStream.write(b);
					fileOuputStream.close();
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}*/
		Error = false;
	}
	/**
	 * Runs the sixth test: Append the size and content of a file stored on the local machine in a target TFS file specified by its path.
	 * Parameters from user input: local file path (string), file (sent in bytes)
	 */
	private void unitSix(){
		String[] path = commands[2].split("/");
		String[] p = new String[path.length-1];
		for (int i = 0; i < p.length; i++) {
			p[i] = path[i];
		}
		//master.createFileNoID(p,path[path.length-1],true,false);//HERE
		outgoingMessage.setMessageType(TFSMessage.mType.CREATEFILE);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(p[p.length-1]);
		File file = new File(commands[1]);
		try {
			RandomAccessFile f = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)f.length()];
			f.read(b);
			System.out.println("Client: Sending appendToFile request to Master.");
			//master.appendToFileWithSize(path,b);//HERE
			outgoingMessage.setMessageType(TFSMessage.mType.APPEND);
			outgoingMessage.setPath(path);
			outgoingMessage.setBytes(b);
		}
		catch (IOException e) {
			System.out.println("Client: Error, can't locate local file path.");
			e.printStackTrace();
		}
	}
	/**
	 * Runs the seventh test: Count the number of logical files stored in a TFS file using Test6 and printout the results.
	 * Parameter from user input: path (string)
	 */
	private void unitSeven(){
		String[] path = commands[1].split("/");
		outgoingMessage.setMessageType(TFSMessage.mType.COUNTFILES);
		outgoingMessage.setPath(path);
		/*if(master.countFilesInNode(path) > 0) {//HERE
			System.out.println("Client: " + master.countFilesInNode(path) + " file/s stored in TFS file.");//HERE
		}*/
	}
	
	private void unitEight(){
		String[] path = commands[1].split("/");
	}
	/**
	 * Sends append message to master
	 * Parameters from user input: path (string), file (in bytes)
	 */
	private void appendFile(){
		String []d=commands[1].split("/");
		File file = new File(commands[2]);
		try {
			RandomAccessFile f = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)f.length()];
			f.read(b);
			//master.appendToFileWithSize(d,b);
			outgoingMessage.setMessageType(TFSMessage.mType.APPEND);
			outgoingMessage.setPath(d);
			outgoingMessage.setBytes(b);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Send make directory message to master
	 * Parameters from user input: path(string)and file name (string)
	 */
	private void makeDirectory(){
		String []d=commands[1].split("/");
		String[]path = new String[d.length-1];
		for (int i = 0; i < d.length-1; i++) {
			path[i] = d[i];
		}
		System.out.println("Client: Sending createDirectory request to Master.");
		//master.createDirectory(path, d[d.length-1],true);
		outgoingMessage.setMessageType(TFSMessage.mType.CREATEDIRECTORY);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
	}
	/**
	 * Send make directory message to master
	 * Parameters from user input: path(string)and file name (string)
	 * @param string path
	 */
	private void makeDirectory(String[] d) {
		String[]path = new String[d.length-1];
		for (int i = 0; i < d.length-1; i++) {
			path[i] = d[i];
		}
		System.out.println("Client: Sending createDirectory request to Master.");
		//master.createDirectory(path, d[d.length-1],true);
		outgoingMessage.setMessageType(TFSMessage.mType.CREATEDIRECTORY);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
	}
	/**
	 * Send make directory message to master
	 * Parameters from user input: path(string)and file name (string)
	 * @param string filename
	 */
	private void makeDirectory(String name){
		directories.remove(name);
		String []d=name.split("/");
		String[]path = new String[d.length-1];
		for (int i = 0; i < d.length-1; i++) {
			path[i] = d[i];
		}
		System.out.println("Client: Sending createDirectory request to Master.");
		//master.createDirectory(path, d[d.length-1],true);
		outgoingMessage.setMessageType(TFSMessage.mType.CREATEDIRECTORY);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
	}
	/**
	 * Send seek file message to master
	 * Parameters from user input: path(string), offset, and file (in bytes)
	 */
	private void seekFile(){
		String []d=commands[1].split("/");
		File file = new File(commands[2]);
		int offset = Integer.parseInt(commands[3]);
		try {
			RandomAccessFile f = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)f.length()];
			f.read(b);
			//master.seek(d,offset,b);
			outgoingMessage.setMessageType(TFSMessage.mType.SEEK);
			outgoingMessage.setPath(d);
			outgoingMessage.setOffset(offset);
			outgoingMessage.setBytes(b);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		//master.seek(d,this);
	}
	/**
	 * Send delete message to master
	 * Parameters from user input: path(string)
	 */
	private void remove(){
		String[]d = commands[1].split("/");
		String[]path = new String[d.length-1];
		for (int i = 0; i < path.length; i++) {
			path[i] = d[i];
		}
		//master.recursiveDelete(d, true);
		outgoingMessage.setMessageType(TFSMessage.mType.DELETE);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
		//master.delete(d, this);
	}
	/**
	 * Send create file message to master
	 * Parameters from user input: path(string)and file name (string)
	 */
	private void createFile(){
		String[]d=commands[1].split("/");
		int r = Integer.parseInt(commands[2]);
		String[]path = new String[d.length-1];
		for (int i = 0; i < d.length-1; i++) {
			path[i] = d[i];
		}
		//master.createFileNoID(path, d[d.length-1], true, true);
		outgoingMessage.setMessageType(TFSMessage.mType.CREATEFILE);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
		outgoingMessage.setReplicaNum(r);
		System.out.println("Create File");
	}
	//Messages
	public void hereIsChunk(String id, String location){
		
	}
	
	public void hereIsData(String File){
		
	}
	/**
	 * Message that directory made was successful
	 * Prints out console again
	 */
	public void DirectoryMade(){
		System.out.println("Directory Made");
		console();
	}
	/**
	 * Message that directory deleted was successful
	 * Prints out console again
	 */
	public void DirectoryDeleted() {
		System.out.println("Directory Deleted");
		console();
	}
	/**
	 * Message that files added was successful
	 * Prints out console again
	 */
	public void FilesAdded() {
		System.out.println("Files Added");
		console();
	}
	/**
	 * Message that there was an error
	 * Prints out console again
	 */
	public void Error(){
		System.out.println("Client: Error, request not completed.");
		Error = true;
		console();
	}
	/**
	 * Message that files stored was successful
	 * Prints out console again
	 */
	public void storedFiles() {
		System.out.println("Files Stored");
		console();
	}
	/**
	 * Gets bytes
	 */
	public void hereAreBytes(byte[] file) {
		FileOutputStream fos=null;
		try {
			fos = new FileOutputStream("earth.jpg");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			fos.write(file);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Message that bytes appended successfully
	 * Prints out console again
	 */
	public void appendedBytes() {
		System.out.println("Done Appending");
		console();
	}
	/**
	 * Message that counted inner files
	 * Prints out console again
	 * @param integer number of files
	 */
	public void countedInnerFiles(int i) {
		System.out.println("Found "+i+" inner files");
		console();
	}
	/**
	 * Message that seeking was successful
	 * Prints out console again
	 */
	public void doneSeeking() {
		System.out.println("Done Seeking");
		console();
	}
	/**
	 * Message that client request was completed
	 */
	public void complete() {
		//if (master != null) {
			System.out.println("Client: Request completed.");
			//console();
		//}
	}
	/**
	 * Message that there was an error with client request
	 */
	public void error() {
		System.out.println("Client: Error, request not completed.");
		Error = true;
		//if (master != null)
			//console();
	}
	/**
	 * Sending message to master
	 */
	private void sendMessageToMaster(){
		try (
            Socket messageSocket = new Socket(hostName, portNumber);
            ObjectOutputStream out = new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			outgoingMessage.sendMessage(out); //send the message to the server
			try { Thread.sleep(100); } catch(InterruptedException e) {}
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
			Error = true;
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName);
			Error = true;
        } 
	}
	/**
	 * Waiting for messages
	 */
	private void listenForResponse()throws IOException, ClassNotFoundException{
	/*Throw this method in before calling console again*/
		System.out.println("Listening for response");
		try { Thread.sleep(1000); } catch(InterruptedException e) {}
		System.out.println(incomingMessage.getMessageType().toString());
		
			ServerSocket serverSocket = new ServerSocket(portNumber);
			serverSocket.setSoTimeout(10000);
			Socket clientSocket = serverSocket.accept();
			//clientSocket.setSoTimeout(10000);
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()); //Receive messages from the client
            
			//we could use a timer to keep it from hanging indefintely
			System.out.println(incomingMessage.getMessageType().toString());
			while(incomingMessage.getMessageType() == TFSMessage.mType.NONE){
				incomingMessage.receiveMessage(in); //call readObject 
			}
			serverSocket.close();
        //} 
		if(incomingMessage.getMessageType() != TFSMessage.mType.NONE)
			parseMessage(incomingMessage);
		else if (incomingMessage.getMessageType() == TFSMessage.mType.ERROR)
			System.out.println("There was an error");
		else if (incomingMessage.getMessageType() == TFSMessage.mType.SUCCESS)
			System.out.println("Transaction Successful");
		else{
			System.out.println("Master cannot be reached");
			console();
		}
	}
	/**
	 * Parsing messages
	 * @param Message
	 */
	private void parseMessage(TFSMessage t){
		//read the data from TFSMessage and call the appropriate response 
		switch(t.getMessageType()){
			case ERROR:
				error();
				break;
			case SUCCESS:
				complete();
				break;
			default:
				System.out.println("done");
				break;
		}
	}
	/**
	 * Send a request of readfile
	 * @param string request, bytes b
	 */
	/*private void sendRequest(String request, byte[] b){
		outgoingMessage.setMessageType(TFSMessage.mType.READFILE);
		outgoingMessage.setBytes(b);
		outgoingMessage.setLocation(request);
	}*/
	/**
	 * The main
	 */
	public static void main (String[]args){
		TFSClient thisClient = new TFSClient();
		thisClient.console();
	}
}