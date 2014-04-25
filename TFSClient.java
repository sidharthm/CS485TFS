import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class TFSClient{
	TFSMaster master;
	String input=null;
	String[] commands=null;
	String location="C: ";
	String chunk_ID=null;
	String chunk_location=null;
	int testone;
	List<String> directories=new ArrayList<String>();
	ArrayList<String[]> paths = new ArrayList<String[]>();
	boolean done=true;
	boolean Error = false;
	String backslash = "\\";
	
	String hostName; //Load this from a file, it stores the master server's IP
	String myName; //This stores the server's own IP so that it can attach that information to messages
	int portNumber = 4444; //Change this if you want to communicate on a different port
	Socket clientMessageSocket; //This is the socket the client uses to contact the master
	
	TFSMessage outgoingMessage;
	TFSMessage incomingMessage;
	
	public TFSClient(){
		setUpClient();
	}
	
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
	
	public void setMaster(TFSMaster m) {
		master = m;
		console();
	}
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
		else if(commands[0].equalsIgnoreCase("Test1")){
			if(commands.length == 2)
				testOne();
		}
		else if(commands[0].equalsIgnoreCase("Test2")){
			if(commands.length == 3)
				testTwo();
		}
		else if(commands[0].equalsIgnoreCase("Test3")){
			if(commands.length == 2)
				testThree();
		}
		else if(commands[0].equalsIgnoreCase("Test4")){
			if(commands.length == 3)
				testFour();
		}
		else if(commands[0].equalsIgnoreCase("Test5")){
			if(commands.length == 3)
				testFive();
		}
		else if(commands[0].equalsIgnoreCase("Test6")){
			if(commands.length == 3)
				testSix();
		}
		else if(commands[0].equalsIgnoreCase("Test7")){
			if(commands.length == 2)
				testSeven();
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
			master.printTree(master.getRoot());
		}
		else if (commands[0].equals("exit"))
			return false;
		else 
			System.out.println("Invalid command");
		sendMessageToMaster();
		listenForResponse();
		//if (flag for needing a server response) listenForResponse()
		console();
		return true;
	}
	
	private void testOne(){
		testone=Integer.parseInt(commands[1]);
		String directory="";
		for (int i = 1; i <= testone; i++) {
			ArrayList<String> strings = new ArrayList<String>();
			int j = i;
			while (j >= 1) {
				strings.add(""+j);
				j /= 2;
			}
			Collections.reverse(strings);
			String[] path = strings.toArray(new String[strings.size()]);
			makeDirectory(path);
		}
		/*
		int i=1;
		int j;
		while(i<=testone){
			j=i;
			while(j/2>1){
				directory=directory+(j/2)+"/";
				j=j/2;
			}
			directory=directory+(i);
			directories.add(directory);
			directory="1/";
			i++;
		}
		for (int k = 0; k < directories.size(); k++) {
			makeDirectory(directories.get(0));
		}
		*/
	}
	
	private void testTwo(){
		String[] d=commands[1].split("/");
		int num = Integer.parseInt(commands[2]);
		System.out.println("Client: Sending createFiles request to Master.");
		master.recursiveCreateFileInitial(d, true, num);
	}
	
	private void testThree(){
		String[] delete=commands[1].split("/");
		System.out.println("Client: Sending deleteDirectories request to Master.");
		master.recursiveDelete(delete,true);
	}
	
	private void testFour(){
		String[] path = commands[2].split("/");
		String[] p = new String[path.length-1];
		for (int i = 0; i < p.length; i++) {
			p[i] = path[i];
		}
		if (master.createFileNoID(p,path[path.length-1],true,true)) {
			File file = new File(commands[1]);
			try {
				RandomAccessFile f = new RandomAccessFile(file, "r");
				byte[] b = new byte[(int)f.length()];
				f.read(b);
				System.out.println("Client: Sending copyFile request from local to TFS request to Master.");
				master.appendToFileNoSize(path,b);
			}
			catch (IOException e) {
				System.out.println("Client: Error, can't locate local file path.");
				e.printStackTrace();
			}
		}
		//else
		//	System.out.println(" File exists in TFS.");
	}
	
	private void testFive(){
		String[]d=commands[1].split("/");
		System.out.println("Client: Sending getFile request from TFS to local to Master.");
		byte[] b = master.readFileNoSize(d);
		if(!Error) {
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
		}
		Error = false;
	}
	
	private void testSix(){
		String[] path = commands[2].split("/");
		String[] p = new String[path.length-1];
		for (int i = 0; i < p.length; i++) {
			p[i] = path[i];
		}
		master.createFileNoID(p,path[path.length-1],true,false);
		File file = new File(commands[1]);
		try {
			RandomAccessFile f = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)f.length()];
			f.read(b);
			System.out.println("Client: Sending appendToFile request to Master.");
			master.appendToFileWithSize(path,b);
		}
		catch (IOException e) {
			System.out.println("Client: Error, can't locate local file path.");
			e.printStackTrace();
		}
	}
	
	private void testSeven(){
		String[] path = commands[1].split("/");
		if(master.countFilesInNode(path) > 0) {
			System.out.println("Client: " + master.countFilesInNode(path) + " file/s stored in TFS file.");
		}
	}
	
	private void appendFile(){
		String []d=commands[1].split("/");
		File file = new File(commands[2]);
		try {
			RandomAccessFile f = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)f.length()];
			f.read(b);
			//master.appendToFileWithSize(d,b);
			outgoingMessage.setPath(d);
			outgoingMessage.setBytes(b);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
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
	
	private void remove(){
		String[]d = commands[1].split("/");
		//master.recursiveDelete(d, true);
		outgoingMessage.setMessageType(TFSMessage.mType.DELETE);
		outgoingMessage.setPath(d);
		//master.delete(d, this);
	}
	
	private void createFile(){
		String[]d=commands[1].split("/");
		String[]path = new String[d.length-1];
		for (int i = 0; i < d.length-1; i++) {
			path[i] = d[i];
		}
		//master.createFileNoID(path, d[d.length-1], true, true);
		outgoingMessage.setMessageType(TFSMessage.mType.CREATEFILE);
		outgoingMessage.setPath(path);
		outgoingMessage.setFileName(d[d.length-1]);
		System.out.println("Create File");
	}
	//Messages
	public void hereIsChunk(String id, String location){
		
	}
	
	public void hereIsData(String File){
		
	}
	
	public void DirectoryMade(){
		System.out.println("Directory Made");
		console();
	}
	public void DirectoryDeleted() {
		System.out.println("Directory Deleted");
		console();
	}
	public void FilesAdded() {
		System.out.println("Files Added");
		console();
	}
	
	public void Error(){
		System.out.println("Client: Error, request not completed.");
		Error = true;
		console();
	}
	public void storedFiles() {
		System.out.println("Files Stored");
		console();
	}
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
	public void appendedBytes() {
		System.out.println("Done Appending");
		console();
	}
	public void countedInnerFiles(int i) {
		System.out.println("Found "+i+" inner files");
		console();
	}
	public void doneSeeking() {
		System.out.println("Done Seeking");
		console();
	}
	
	public void complete() {
		//if (master != null) {
			System.out.println("Client: Request completed.");
			//console();
		//}
	}
	
	public void error() {
		System.out.println("Client: Error, request not completed.");
		Error = true;
		//if (master != null)
			//console();
	}
	private void sendMessageToMaster(){
		try (
            Socket messageSocket = new Socket(hostName, portNumber);
            ObjectOutputStream out = new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			outgoingMessage.sendMessage(out); //send the message to the server
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
			Error = true;
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName);
			Error = true;
        } 
	}
	private void listenForResponse(){
	/*Throw this method in before calling console again*/
		System.out.println("Listening for response");
		System.out.println(incomingMessage.getMessageType().toString());
		try (
			ServerSocket serverSocket = new ServerSocket(portNumber);
			Socket clientSocket = serverSocket.accept();
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()); //Receive messages from the client
        ) {
			//we could use a timer to keep it from hanging indefintely
			System.out.println(incomingMessage.getMessageType().toString());
			while(incomingMessage.getMessageType() == TFSMessage.mType.NONE){
				System.out.print("l");
				incomingMessage.receiveMessage(in); //call readObject 
			}
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
	
	public static void main (String[]args){
		TFSClient thisClient = new TFSClient();
		thisClient.console();
	}
}