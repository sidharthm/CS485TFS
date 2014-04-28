import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class TFSMaster implements Runnable {
	/**
	 * Since the directory structure is essentially a tree, we store the root of the tree
	 */
	//TFSNode root;
	/**
	 * The TFS client
	 */
	//TFSClient client;
	/**
	 * The log file we write all file structure changes to
	 */
	File logfile;
	
	TFSMasterSwitchboard switchboard;
	
	boolean killProcess = false;
	
	public enum ErrorType { PATH, LOCKING; }
	
	ErrorType errorType;
	
	private List<TFSMessage> incomingMessages; // This Queue will store any incoming messages
	
	Object lock;
	
	String waitingIP;
	

	public TFSMaster(TFSMasterSwitchboard s, Object l){
		/*Set up all messages with the appropriate initialization*/
		incomingMessages = Collections.synchronizedList(new ArrayList<TFSMessage>());
		//root = new TFSNode(false,null,-1,"root");
		switchboard = s;
		lock = l;
	}

	/**
	 * Constructor for the TFS Master without server. Sets the client and creates the log file if it doesn't exist. If
	 * it does exist, it reads from the log file to get the current structure
	 * @param c The client that the master connects to
	 */
	public TFSMaster(TFSClient c) {
		//Create the root. Not a file, has no parent, id of -1 (because all time stamps are positive, so
		//new files and directories will have positive IDs), and a name of root
		//root = new TFSNode(false,null,-1,"root");
		//client = c;
	}
	
	public void run() {
		while (true) {
			scheduler();
		}
	}
	
	public void scheduler() {
		if (incomingMessages.size() > 0) {
			TFSMessage message;
			synchronized(incomingMessages) {
				message = incomingMessages.remove(0);
			}
			TFSMessage.mType type = message.getMessageType();
			if (type == TFSMessage.mType.APPEND) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				byte[] data = message.getBytes();
				appendToFileNoSize(path,data);
			}
			else if (type == TFSMessage.mType.COUNTFILES) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				countFilesInNode(path);
			}
			else if (type == TFSMessage.mType.CREATEDIRECTORY) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				createDirectory(p1,p2,true);
			}
			else if (type == TFSMessage.mType.CREATEFILE) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				int replicas = message.getReplicaNum();
				createFileNoID(p1,p2,true,true,replicas);
			}
			else if (type == TFSMessage.mType.DELETE) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				recursiveDeleteInitial(path,true);
			}
			else if (type == TFSMessage.mType.INITIALIZE) {
				initializeStructure();
			}
			else if (type == TFSMessage.mType.PRINT) {
				printTree(getRoot());
			}
			else if (type == TFSMessage.mType.READFILE) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				readFileNoSize(path);
			}
			else if (type == TFSMessage.mType.RECURSIVECREATE) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				int num = message.getFileNum();
				int replicas = message.getReplicaNum();
				recursiveCreateFileInitial(path,true,num,replicas);
			}
			else if (type == TFSMessage.mType.SEEK) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				int offset = message.getOffset();
				byte[] data = message.getBytes();
				seekAndWrite(path,offset,data);
			}
			else if (type == TFSMessage.mType.SIZEDAPPEND) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				byte[] data = message.getBytes();
				appendToFileWithSize(path,data);
			}
		}
		try { Thread.sleep(10); } catch(InterruptedException e) {}
	}
	
	public void addMessage(TFSMessage m) {
		incomingMessages.add(m);
	}
	
	public void sendMessage(TFSMessage m) {
		switchboard.addOutgoingMessage(m);
	}
	
	/**
	 * Getter method for the root
	 * @return the TFSNode root
	 */
	public TFSNode getRoot() {
		return switchboard.getRoot();
		//return client.root;
	}
	
	public String getIP() {
		return waitingIP;
	}
	
	public Object getLock() {
		return lock;
	}
	
	public void setKillProcess(boolean k) {
		killProcess = k;
	}
	
	public void initializeStructure() {
		File file = new File("local/");
		file.mkdirs();
		//Create a new log file or read the current one
		logfile = new File("log.txt");
		try {
			if (!logfile.isFile())
				logfile.createNewFile();
			else
				readLogFile();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Internal Method. Should only be called within the Master
	 * Searches the directory structure for the specified TFSNode based on a given path
	 * @param path An array of directory names (and potentially one file name at the end) that indicates
	 * the path
	 * @param index
	 * The current index of the path array we are searching
	 * @param node The current node we are on in the search
	 * @param first
	 * @param second
	 * @return The TFSNode of the specified path or null if the path does not exist
	 */
	public TFSNode searchTree(String[] path, int index, TFSNode node, NodeLock first, NodeLock second, boolean addLock) {
		synchronized(node) {
		//We have gone through every directory/file in the given path so the current node is the
		//correct node
		if (index == path.length) {
			//Since we are at the final node, we want to check it with the second lock to make sure the
			//lock is valid
			if (addLock && node.checkLocks(second)) {
				node.getLock().add(second);
				return node;
			}
			else if (!addLock)
				return node;
			else {
				int i = index - 1;
				if (i > 0) {
					String p2[] = new String[i];
					for (int j = 0; j < p2.length; j++) {
						p2[j] = path[j];
					}
					removeLocks(p2,0,getRoot(),first,second);
				}
				else {
					String p3[] = new String[0];
					removeLocks(p3,0,getRoot(),first,first);
					removeLocks(p3,0,getRoot(),second,second);
				}
				errorType = ErrorType.LOCKING;
				return null;
			}
		}
		else {
			TFSNode tempNode = null;
			TFSNode newNode = null;
			//Search all of the nodes file/directory children for the next item in the path
			for (int i = 0; i < node.getChildren().size(); i++) {
				//Correct Node found
				if (node.getChildren().get(i).getName().equals(path[index])) {
					synchronized(node.getChildren().get(i)) {
					tempNode = node.getChildren().get(i);
					if ((addLock && tempNode.checkLocks(first))) {
						newNode = tempNode;
						node.getLock().add(first);
					}
					else if (!addLock)
						newNode = tempNode;
					else {
						int k = index - 1;
						if (k > 0) {
							String p2[] = new String[k];
							for (int j = 0; j < p2.length; j++) {
								p2[j] = path[j];
							}
							removeLocks(p2,0,getRoot(),first,second);
						}
						else {
							String p3[] = new String[0];
							removeLocks(p3,0,getRoot(),first,first);
							removeLocks(p3,0,getRoot(),second,second);
						}
						errorType = ErrorType.LOCKING;
					}
					break;
				}
				}
			}
			//Update current node and increment index and call recursion
			if (newNode != null) {
				return searchTree(path,index+1,newNode,first,second,addLock);
			}
			//No node found. Invalid path
			else {
				//System.out.println("Path does not exist");
				int k = index - 1;
				if (k > 0) {
					String p2[] = new String[k];
					for (int j = 0; j < p2.length; j++) {
						p2[j] = path[j];
					}
					removeLocks(p2,0,getRoot(),first,second);
				}
				else {
					String p3[] = new String[0];
					removeLocks(p3,0,getRoot(),first,first);
					removeLocks(p3,0,getRoot(),second,second);
				}
				errorType = ErrorType.PATH;
				return null;
			}
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param index
	 * @param node
	 * @param first
	 * @param second
	 */
	public void removeLocks(String[] path, int index, TFSNode node, NodeLock first, NodeLock second) {
		synchronized (node) {
		if (index == path.length) {
			//Since we are at the final node, we want to remove the second lock
			node.removeLock(second);
		}
		else {
			TFSNode newNode = null;
			//Search all of the nodes file/directory children for the next item in the path
			for (int i = 0; i < node.getChildren().size(); i++) {
				//Correct Node found
				if (node.getChildren().get(i).getName().equals(path[index])) {
					newNode = node.getChildren().get(i);
					//Not the final node so remove the first kind of lock
					node.removeLock(first);
					break;
				}
			}
			//Update current node and increment index and call recursion
			if (newNode != null) {
				removeLocks(path,index+1,newNode,first,second);
			}
			//No node found. Invalid path
			else {
				System.out.println("Master: (Internal Error) Bad path");
			}
		}
		}
	}

	public void removeReplicas(TFSNode node) {
		synchronized(node) {
			if (node.getIsFile()) {
				for (int i = 0; i < node.getReplicas().size(); i++) {
					if (switchboard.getChunkServers().indexOf(node.getReplicas().get(i)) == -1) {
						node.getReplicas().remove(i);
					}
				}
			}
			else {
				for (int i = 0; i < node.getChildren().size(); i++) {
					removeReplicas(node.getChildren().get(i));
				}
			}
		}
	}
	
	public void addReplica(String IP, TFSNode node) {
		synchronized(node) {
			if (node.getIsFile()) {
				if (node.getReplicas().size() < node.getDesiredReplicas()) {
					TFSMessage m = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
					m.setMessageType(TFSMessage.mType.CREATEFILE);
					m.setDestination(IP);
					m.setFileID(node.getId());
					switchboard.addOutgoingMessage(m);
					synchronized(lock) {
						try {
							waitingIP = IP;
							wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					if (!killProcess) {
						node.getReplicas().add(IP);
					}
					else {
						killProcess = false;
						return;
					}
				}
			}
			else {
				for (int i = 0; i < node.getChildren().size(); i++) {
					addReplica(IP,node.getChildren().get(i));
				}
			}
		}
	}
	/*
	This method was written before but is now obsolete. Commented out just in case
	public boolean recursiveCreateFile(String[] path, String name, boolean write) {
		TFSNode directory = searchTree(path,0,root);
		if (directory == null) {
			System.out.println("Master: Error, no directory exists.");
			return false;
		}
		else if (directory.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			return false;
		}
		else {
			boolean cont = true;
			for (int i = 0; i < directory.getChildren().size(); i++) {
				if (directory.getChildren().get(i).getName().equals(name)) {
					System.out.println("Master: Error, file already exists.");
					cont = false;
				}
			}
			if (cont) {
				long id = System.currentTimeMillis();
				TFSNode newNode = new TFSNode(true,directory,id,name);
				directory.getChildren().add(newNode);
				try {
					File file = new File("local/" + id);
					file.createNewFile();
					if (write)
						writeLogEntry("createFileID",path,name,id);
					return true;
				}
				catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
			return false;
		}
	}
	*/
	
	/**
	 * Internal Method. Is only called by readLogFile()
	 * Creates the specified file if an ID for it already exists (i.e. the file existed when the program
	 * was shut down and now needs to be recreated on reboot).
	 * @param path The path of the file we are creating
	 * @param name The name of the file we are creating
	 * @param write Whether we want to write this to the log (note: because we only call this from
	 * readLogFile(), we don't want to rewrite this create to the log, so this should always be false)
	 * @param oldID The id for the file we are creating
	 * @return True if file created. False if path is incorrect or file already exists
	 */
	public boolean createFileWithID(String[] path, String name, boolean write, long oldID, int replicas) {
		String[] servers;
		synchronized (switchboard.getChunkServers()) {
			if (replicas > switchboard.getChunkServers().size()) {
				return false;
			}
			else {
				servers = chooseChunkServers(replicas);
			}
		}
		//Find the correct node
		TFSNode directory = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		//Path does not exist
		if (directory == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			return false;
		}
		else if (directory == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(directory) {
		//End of path is a file not directory
		if (directory.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			return false;
		}
		else {
			boolean cont = true;
			//Check directory's children
			for (int i = 0; i < directory.getChildren().size(); i++) {
				//Directory already has this file
				if (directory.getChildren().get(i).getName().equals(name)) {
					System.out.println("Master: Error, file already exists.");
					cont = false;
				}
			}
			//File wasn't found. Create it
			if (cont) {
				long id = oldID;
				TFSNode newNode = new TFSNode(true,directory,id,name,replicas);
				directory.getChildren().add(newNode);
				/*
				synchronized(lock) {
					try {
						waitingIP = IP;
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				*/
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				return true;
			}
			return false;
		}
		}
	}
	
	public String[] chooseChunkServers(int n) {
		synchronized (switchboard.getChunkServers()) {
			ArrayList<String> shuffledList = new ArrayList<String>();
			shuffledList = new ArrayList<String>(switchboard.getChunkServers());
			Collections.shuffle(shuffledList);
			String[] servers = new String[n];
			for (int i = 0; i < servers.length; i++) {
				servers[i] = shuffledList.get(i);
			}
			return servers;
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param name
	 * @param write
	 * @param error
	 * @return
	 */
	public boolean createFileNoID(String[] path, String name, boolean write, boolean error, int replicas) {
		String[] servers;
		synchronized (switchboard.getChunkServers()) {
			if (replicas > switchboard.getChunkServers().size()) {
				return false;
			}
			else {
				servers = chooseChunkServers(replicas);
			}
		}
		TFSNode directory = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (directory == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//if (error)
				//client.error();
			return false;
		}
		else if (directory == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(directory) {
		if (directory.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			//if (error)
				//client.error();
			return false;
		}
		else {
			boolean cont = true;
			for (int i = 0; i < directory.getChildren().size(); i++) {
				if (directory.getChildren().get(i).getName().equals(name)) {
					if(!write || error){	//Not appending to file
						System.out.println("Master: Error, file already exists.");
					}
					cont = false;
				}
			}
			if (cont) {
				long id = System.currentTimeMillis();
				TFSNode newNode = new TFSNode(true,directory,id,name,replicas);
				directory.getChildren().add(newNode);
				for (int i = 0; i < servers.length; i++) {
					TFSMessage m = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
					m.setMessageType(TFSMessage.mType.CREATEFILE);
					m.setDestination(servers[i]);
					m.setFileID(id);
					switchboard.addOutgoingMessage(m);
					synchronized(lock) {
						try {
							waitingIP = servers[i];
							lock.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				if (!killProcess) {
					try {
						removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
						if (write)
							writeLogEntry("createFileID",path,name,id);
						//client.complete();
						return true;
					}
					catch (Exception e) {
						e.printStackTrace();
					}
				}
				else {
					removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
					killProcess = false;
				}
			}
			//if (error)
				//client.error();
			return false;
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param name
	 * @param write
	 * @return
	 */
	public boolean createDirectory(String[] path, String name, boolean write) {
		TFSNode directory = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (directory == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, directory does not exist.");
			TFSMessage errorMSG = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
			errorMSG.setMessageType(TFSMessage.mType.ERROR);
			for (int i = 0; i < switchboard.getClients().size(); i++){
				
				errorMSG.setDestination(switchboard.getClients().get(i));
				sendMessage(errorMSG);
			}
			//client.error();
			return false;
		}
		else if (directory == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			TFSMessage errorMSG = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
			errorMSG.setMessageType(TFSMessage.mType.ERROR);
			for (int i = 0; i < switchboard.getClients().size(); i++){
				
				errorMSG.setDestination(switchboard.getClients().get(i));
				
				sendMessage(errorMSG);
			}
			return false;
		}
		synchronized(directory) {
		if (directory.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			TFSMessage errorMSG = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
			errorMSG.setMessageType(TFSMessage.mType.ERROR);
			for (int i = 0; i < switchboard.getClients().size(); i++){
				errorMSG.setDestination(switchboard.getClients().get(i));
				sendMessage(errorMSG);
			}
			//client.error();
			return false;
		}
		else {
			boolean cont = true;
			for (int i = 0; i < directory.getChildren().size(); i++) {
				if (directory.getChildren().get(i).getName().equals(name)) {
					System.out.println("Master: Directory already exists.");
					TFSMessage errorMSG = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
					errorMSG.setMessageType(TFSMessage.mType.ERROR);
					for (int j = 0; j < switchboard.getClients().size(); j++){
						errorMSG.setDestination(switchboard.getClients().get(j));
						sendMessage(errorMSG);
					}
					cont = false;
				}
			}
			if (cont) {
				TFSNode newNode = new TFSNode(false,directory,0,name,0);
				directory.getChildren().add(newNode);
				if (write)
					writeLogEntry("createDirectory",path,name,-1,-1);
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				TFSMessage successMSG = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
				successMSG.setMessageType(TFSMessage.mType.SUCCESS);
				for (int i = 0; i < switchboard.getClients().size(); i++){
					successMSG.setDestination(switchboard.getClients().get(i));
					sendMessage(successMSG);
				}
				//client.complete();
				return true;
			}
			//client.error();
			return false;
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param write
	 * @return
	 */
	public boolean deleteFile(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists,");
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
		if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			return false;
		}
		else {
			for (int i = 0; i < file.getReplicas().size(); i++) {
				TFSMessage m = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
				m.setMessageType(TFSMessage.mType.DELETE);
				m.setDestination(file.getReplicas().get(i));
				m.setFileID(file.getId());
				switchboard.addOutgoingMessage(m);
				synchronized(lock) {
					try {
						waitingIP = file.getReplicas().get(i);
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			String[] path2 = new String[path.length-1];
			for (int i = 0; i < path2.length; i++) {
				path2[i] = path[i];
			}
			if (!killProcess) {
				deleteFileNode(file);
				if (write)
					writeLogEntry("deleteFile",path,null,-1,-1);
				removeLocks(path2,0,getRoot(),new NodeLock("IX"),new NodeLock("IX"));
				return true;
			}
			else {
				removeLocks(path2,0,getRoot(),new NodeLock("IX"),new NodeLock("IX"));
				killProcess = false;
				return false;
			}
		}
		}
	}
	
	public boolean deleteDirectory(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists,");
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
		if (file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			return false;
		}
		else {
			deleteDirectoryNode(file);
			if (write)
				writeLogEntry("deleteDirectory",path,null,-1,-1);
			String[] path2 = new String[path.length-1];
			for (int i = 0; i < path2.length; i++) {
				path2[i] = path[i];
			}
			removeLocks(path2,0,getRoot(),new NodeLock("IX"),new NodeLock("IX"));
			return true;
		}
		}
	}
	/**
	 * 
	 * @param file
	 * @return
	 */
	public boolean deleteFileNode(TFSNode file) {
		synchronized(file) {
		file.getParent().getChildren().remove(file);
		long id = file.getId();
		try {
			File jfile = new File("local/" + id);
			jfile.delete();
			file = null;
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return false;
		}
	}

	/**
	 * 
	 * @param directory
	 */
	public void deleteDirectoryNode(TFSNode directory) {
		synchronized(directory) {
			directory.getParent().getChildren().remove(directory);
			directory = null;
		}
	}
	
	/*
	public void recursiveDeleteFromNode(TFSNode node) {
		synchronized(node) {
		for (int i = 0; i < node.getChildren().size(); i++) {
			if (node.getChildren().get(i).getIsFile()) {
				deleteFileNode(node.getChildren().get(i));
			}
			else if (node.getChildren().get(i).getChildren().size() == 0) {
				deleteDirectoryNode(node.getChildren().get(i));
			}
			else {
				node.getLock().add(new NodeLock("IX"));
				recursiveDeleteFromNode(node.getChildren().get(i));
				node.removeLock(new NodeLock("IX"));
			}
		}
		if(node.getIsFile()) {
			deleteFileNode(node);
		}
		else {
			deleteDirectoryNode(node);
		}
		}
	}
	*/
	
	/**
	 * 
	 * @param path
	 * @param write
	 * @param num
	 * @return
	 */
	public boolean recursiveCreateFileInitial(String[] path, boolean write, int num, int replicas) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),false);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
		if (file.getIsFile()) {
			System.out.println("Master: Error, not a directory.");
			//client.error();
			return false;
		}
		else {
			recursiveCreateFile(path, file, write, num, replicas);
			//if (write)
				//writeLogEntry("recursiveCreate",path,null,(long)num);
			//client.complete();
			return true;
		}
		}
	}
	
	public boolean recursiveDeleteInitial(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),false);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
			recursiveDeleteFile(path, file, write);
			//if (write)
				//writeLogEntry("recursiveCreate",path,null,(long)num);
			//client.complete();
			return true;
		}
	}
	
	public void recursiveDeleteFile(String[] path, TFSNode node, boolean write) {
		synchronized(node) {
			String[] path2 = new String[path.length+1];
			for (int i = 0; i < path.length; i++) {
				path2[i] = path[i];
			}
			for (int i = 0; i < node.getChildren().size(); i++) {
				synchronized(node.getChildren().get(i)) {
					TFSNode newNode = node.getChildren().get(i);
					path2[path2.length-1] = newNode.getName();
					recursiveDeleteFile(path2, newNode, true);
					i = i-1;
				}
			}
			if (node.getIsFile()) {
				deleteFile(path,true);
			}
			else {
				deleteDirectory(path,true);
			}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param node
	 * @param write
	 * @param num
	 */
	public void recursiveCreateFile(String[] path, TFSNode node, boolean write, int num, int replicas) {
		synchronized(node) {
		String[] path2 = new String[path.length+1];
		for (int i = 0; i < path.length; i++) {
			path2[i] = path[i];
		}
		for (int i = 0; i < node.getChildren().size(); i++) {
			TFSNode newNode = node.getChildren().get(i);
			if (!newNode.getIsFile()) {
				synchronized(node.getChildren().get(i)) {
				path2[path2.length-1] = newNode.getName();
				recursiveCreateFile(path2, newNode, true, num, replicas);
				}
			}
		}
		for (int i = 0; i < num; i++) {
			int j = i+1;
			String name = "File" + j;
			createFileNoID(path,name,true,true,replicas);
			try {
				Thread.sleep(1);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		}
	}
	/*
	public boolean recursiveDelete(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("IX"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error no directory exists.");
			client.error();
			return false;
		}
		else if (file.getIsFile()) {
			removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("IX"));
			boolean b = deleteFile(path,write);
			if (!b) {
				client.error();
				return false;
			}
			else {
				client.complete();
				return true;
			}
		}
		else {
			file.removeLock(new NodeLock("IX"));
			recursiveDeleteFromNode(file);
			if (write)
				writeLogEntry("recursiveDelete",path,null,-1,-1);
			String[] path2 = new String[path.length-1];
			for (int i = 0; i < path2.length; i++) {
				path2[i] = path[i];
			}
			removeLocks(path2,0,getRoot(),new NodeLock("IX"),new NodeLock("IX"));
			client.complete();
			return true;
		}
		}
	}
	*/
	
	/**
	 * 
	 * @param path
	 * @param offset
	 * @param bytes
	 * @return
	 */
	public boolean seekAndWrite(String[] path, int offset, byte[] bytes) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
		if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			//client.error();
			return false;
		}
		else {
			for (int i = 0; i < file.getReplicas().size(); i++) {
				TFSMessage m = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
				m.setMessageType(TFSMessage.mType.SEEK);
				m.setDestination(file.getReplicas().get(i));
				m.setFileID(file.getId());
				m.setOffset(offset);
				m.setBytes(bytes);
				switchboard.addOutgoingMessage(m);
				synchronized(lock) {
					try {
						waitingIP = file.getReplicas().get(i);
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			if (!killProcess) {
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				//client.complete();
				return true;
			}
			else {
				killProcess = false;
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				//client.error();
				return false;
			}
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param bytes
	 * @return
	 */
	public boolean appendToFileWithSize(String[] path, byte[] bytes) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
		if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			//client.error();
			return false;
		}
		else {
			for (int i = 0; i < file.getReplicas().size(); i++) {
				TFSMessage m = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
				m.setMessageType(TFSMessage.mType.SIZEDAPPEND);
				m.setDestination(file.getReplicas().get(i));
				m.setFileID(file.getId());
				m.setBytes(bytes);
				switchboard.addOutgoingMessage(m);
				synchronized(lock) {
					try {
						waitingIP = file.getReplicas().get(i);
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			if (!killProcess) {
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				//client.complete();
				return true;
			}
			else {
				killProcess = false;
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				//client.error();
				return false;
			}
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param bytes
	 * @return
	 */
	public boolean appendToFileNoSize(String[] path, byte[] bytes) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return false;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return false;
		}
		synchronized(file) {
		if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			//client.error();
			return false;
		}
		else {
			for (int i = 0; i < file.getReplicas().size(); i++) {
				TFSMessage m = new TFSMessage(switchboard.getName(),TFSMessage.Type.MASTER);
				m.setMessageType(TFSMessage.mType.APPEND);
				m.setDestination(file.getReplicas().get(i));
				m.setFileID(file.getId());
				m.setBytes(bytes);
				switchboard.addOutgoingMessage(m);
				synchronized(lock) {
					try {
						waitingIP = file.getReplicas().get(i);
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			if (!killProcess) {
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				writeLogEntry("createFile")
				//client.complete();
				return true;
			}
			else {
				killProcess = false;
				removeLocks(path,0,getRoot(),new NodeLock("IX"),new NodeLock("X"));
				//client.error();
				return false;
			}
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	public byte[] readFileNoSize(String[] path) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IS"),new NodeLock("S"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return null;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return null;
		}
		synchronized(file) {
		if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			//client.error();
			return null;
		}
		else {
			/*
			synchronized(lock) {
				try {
					waitingIP = IP;
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			*/
			long id = file.getId();
			try {
				File jfile = new File("local/" + id);
				RandomAccessFile f = new RandomAccessFile(jfile,"r");
				byte[] b = new byte[(int)f.length()];
				f.read(b);
				removeLocks(path,0,getRoot(),new NodeLock("IS"),new NodeLock("S"));
				//client.complete();
				return b;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		//client.error();
		return null;
		}
	}
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	public int countFilesInNode(String[] path) {
		TFSNode file = searchTree(path,0,getRoot(),new NodeLock("IS"),new NodeLock("S"),true);
		if (file == null && errorType == ErrorType.PATH) {
			System.out.println("Master: Error, no directory exists.");
			//client.error();
			return -1;
		}
		else if (file == null && errorType == ErrorType.LOCKING) {
			System.out.println("Master: Directory locked");
			return -1;
		}
		synchronized(file) {
		int count = 0;
		if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			//client.error();
			return -1;
		}
		else {
			/*
			synchronized(lock) {
				try {
					waitingIP = IP;
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			*/
			long id = file.getId();
			try {
				File jfile = new File("local/" + id);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				long delta = 0;
				while (delta < f.length()) {
					f.seek(delta);
					int size = f.readInt();
					delta = delta + 4 + (long)size;
					count++;
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		removeLocks(path,0,getRoot(),new NodeLock("IS"),new NodeLock("S"));
		//client.complete();
		return count;
		}
	}
	
	/**
	 * 
	 * @param node
	 */
	public void printTree(TFSNode node) {
		synchronized(node) {
		for (int i = 0; i < node.getChildren().size(); i++) {
			if (node.getChildren().get(i).getIsFile()) {
				System.out.println("File: " + node.getChildren().get(i).getName() + " (Locks: " + node.getChildren().get(i).printLock() + ")" + " (Replicas: " + node.getChildren().get(i).printReplicas() + ")");
			}
			else {
				printTree(node.getChildren().get(i));
			}
		}
		if(node.getIsFile()) {
			System.out.println("File: " + node.getName() + " (Locks: " + node.printLock() + ")" + " (Replicas: " + node.printReplicas() + ")");
		}
		else {
			System.out.println("Directory: " + node.getName() + " (Locks: " + node.printLock() + ")");
		}
		}
	}
	
	/**
	 * 
	 * @param command
	 * @param path
	 * @param name
	 * @param id
	 */
	public void writeLogEntry(String command, String[] path, String name, long id, int replicas) {
		FileWriter fstream;
		try {
			fstream = new FileWriter("log.txt",true);
			synchronized (fstream) {
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(command);
			out.newLine();
			out.write(""+path.length);
			out.newLine();
			for (int i = 0; i < path.length; i++) {
				out.write(path[i]);
				out.newLine();
			}
			if (name != null) {
				out.write(name);
				out.newLine();
			}
			if (id > 0) {
				out.write(""+id);
				out.newLine();
			}
			if (replicas > 0) {
				out.write(replicas);
				out.newLine();
			}
			out.close();
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 */
	public void readLogFile() {
		try {
			BufferedReader in = new BufferedReader(new FileReader("log.txt"));
			synchronized(in) {
			String line = "";
			while ((line = in.readLine()) != null) {
				if (line.equals("createFileID")) {
					int length = Integer.parseInt(in.readLine());
					String[] path = new String[length];
					for (int i = 0; i < length; i++) {
						path[i]=in.readLine();
					}
					String name = in.readLine();
					long id = Long.parseLong(in.readLine());
					int replicas = Integer.parseInt(in.readLine());
					createFileWithID(path,name,false,id,replicas);
				}
				else if (line.equals("createDirectory")) {
					int length = Integer.parseInt(in.readLine());
					String[] path = new String[length];
					for (int i = 0; i < length; i++) {
						path[i]=in.readLine();
					}
					String name = in.readLine();
					createDirectory(path,name,false);
				}
				else if (line.equals("deleteFile")) {
					int length = Integer.parseInt(in.readLine());
					String[] path = new String[length];
					for (int i = 0; i < length; i++) {
						path[i]=in.readLine();
					}
					deleteFile(path,false);
				}
				else if (line.equals("deleteDirectory")) {
					int length = Integer.parseInt(in.readLine());
					String[] path = new String[length];
					for (int i = 0; i < length; i++) {
						path[i]=in.readLine();
					}
					deleteDirectory(path,false);
				}
				/*
				else if (line.equals("recursiveDelete")) {
					int length = Integer.parseInt(in.readLine());
					String[] path = new String[length];
					for (int i = 0; i < length; i++) {
						path[i]=in.readLine();
					}
					recursiveDelete(path,false);
				}
				else if (line.equals("recursiveCreate")) {
					int length = Integer.parseInt(in.readLine());
					String[] path = new String[length];
					for (int i = 0; i < length; i++) {
						path[i]=in.readLine();
					}
					int num = Integer.parseInt(in.readLine());
					recursiveCreateFileInitial(path,false,num);
				}
				*/
				else {
					System.out.println("Bad log file");
				}
			}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}