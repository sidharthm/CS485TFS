import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class TFSMaster implements Runnable {
	/**
	 * Since the directory structure is essentially a tree, we store the root of the tree
	 */
	TFSNode root;
	/**
	 * The TFS client
	 */
	TFSClient client;
	/**
	 * The log file we write all file structure changes to
	 */
	File logfile;
	
	
	
	private ArrayList<TFSMessage> incomingMessages; // This Queue will store any incoming messages
	

	public TFSMaster(){
		/*Set up all messages with the appropriate initialization*/
		incomingMessages = new ArrayList<TFSMessage>();
		root = new TFSNode(false,null,-1,"root");
	}

	/**
	 * Constructor for the TFS Master without server. Sets the client and creates the log file if it doesn't exist. If
	 * it does exist, it reads from the log file to get the current structure
	 * @param c The client that the master connects to
	 */
	public TFSMaster(TFSClient c) {
		//Create the root. Not a file, has no parent, id of -1 (because all time stamps are positive, so
		//new files and directories will have positive IDs), and a name of root
		root = new TFSNode(false,null,-1,"root");
		client = c;
	}
	
	public void run() {
		if (incomingMessages.size() > 0) {
			TFSMessage message = incomingMessages.remove(0);
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
				createFileNoID(p1,p2,true,true);
			}
			else if (type == TFSMessage.mType.CREATEREPLICA) {
				
			}
			else if (type == TFSMessage.mType.DELETE) {
				String p2 = message.getFileName();
				String[] p1 = message.getPath();
				String[] path = new String[p1.length+1];
				for (int i = 0; i < p1.length; i++) {
					path[i] = p1[i];
				}
				path[p1.length] = p2;
				recursiveDelete(path,true);
			}
			else if (type == TFSMessage.mType.INITIALIZE) {
				initializeStructure();
			}
			else if (type == TFSMessage.mType.PRINT) {
				printTree(root);
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
				recursiveCreateFileInitial(path,true,num);
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
	}
	
	public void addMessage(TFSMessage m) {
		incomingMessages.add(m);
	}
	
	/**
	 * Getter method for the root
	 * @return the TFSNode root
	 */
	public TFSNode getRoot() {
		return root;
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
					removeLocks(p2,0,root,first,second);
				}
				else {
					synchronized(root.getLock()) {
						root.removeLock(first);
						root.removeLock(second);
					}
				}
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
							removeLocks(p2,0,root,first,second);
						}
						else {
							synchronized(root.getLock()) {
								root.removeLock(first);
								root.removeLock(second);
							}
						}
					}
					break;
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
					removeLocks(p2,0,root,first,second);
				}
				else {
					synchronized(root.getLock()) {
						root.removeLock(first);
						root.removeLock(second);
					}
				}
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
	public boolean createFileWithID(String[] path, String name, boolean write, long oldID) {
		//Find the correct node
		TFSNode directory = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(directory) {
		//Path does not exist
		if (directory == null) {
			System.out.println("Master: Error, no directory exists.");
			return false;
		}
		//End of path is a file not directory
		else if (directory.getIsFile()) {
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
				TFSNode newNode = new TFSNode(true,directory,id,name);
				directory.getChildren().add(newNode);
				try {
					File file = new File("local/" + id);
					file.createNewFile();
					//Because write is always false, we should never be rewriting to the log entry
					if (write)
						writeLogEntry("createFileID",path,name,id);
					removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("X"));
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			return false;
		}
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
	public boolean createFileNoID(String[] path, String name, boolean write, boolean error) {
		TFSNode directory = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(directory) {
		if (directory == null) {
			System.out.println("Master: Error, no directory exists.");
			if (error)
				client.error();
			return false;
		}
		else if (directory.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			if (error)
				client.error();
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
				long id = System.currentTimeMillis();;
				TFSNode newNode = new TFSNode(true,directory,id,name);
				directory.getChildren().add(newNode);
				try {
					File file = new File("local/" + id);
					file.createNewFile();
					if (write)
						writeLogEntry("createFileID",path,name,id);
					removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("X"));
					client.complete();
					return true;
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (error)
				client.error();
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
		TFSNode directory = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(directory) {
		if (directory == null) {
			System.out.println("Master: Error, directory unaccessible.");
			client.error();
			return false;
		}
		else if (directory.getIsFile()) {
			System.out.println("Master: Error, end of path is not a directory.");
			client.error();
			return false;
		}
		else {
			boolean cont = true;
			for (int i = 0; i < directory.getChildren().size(); i++) {
				if (directory.getChildren().get(i).getName().equals(name)) {
					System.out.println("Master: Directory already exists.");
					cont = false;
				}
			}
			if (cont) {
				TFSNode newNode = new TFSNode(false,directory,0,name);
				directory.getChildren().add(newNode);
				if (write)
					writeLogEntry("createDirectory",path,name,-1);
				removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("X"));
				client.complete();
				return true;
			}
			client.error();
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
		TFSNode file = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error, no directory exists,");
			return false;
		}
		else if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			return false;
		}
		else {
			deleteFileNode(file);
			if (write)
				writeLogEntry("deleteFile",path,null,-1);
			String[] path2 = new String[path.length-1];
			for (int i = 0; i < path2.length; i++) {
				path2[i] = path[i];
			}
			removeLocks(path2,0,root,new NodeLock("IX"),new NodeLock("IX"));
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
		if (directory.checkLocks(new NodeLock("X"))) {
			directory.getParent().getChildren().remove(directory);
			directory = null;
		}
		else {
			System.out.println("Unable to access directory");
		}
		}
	}
	
	/**
	 * 
	 * @param node
	 */
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
	
	/**
	 * 
	 * @param path
	 * @param write
	 * @param num
	 * @return
	 */
	public boolean recursiveCreateFileInitial(String[] path, boolean write, int num) {
		TFSNode file = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),false);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return false;
		}
		else if (file.getIsFile()) {
			System.out.println("Master: Error, not a directory.");
			client.error();
			return false;
		}
		else {
			recursiveCreateFile(path, file, write, num);
			//if (write)
				//writeLogEntry("recursiveCreate",path,null,(long)num);
			client.complete();
			return true;
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
	public void recursiveCreateFile(String[] path, TFSNode node, boolean write, int num) {
		synchronized(node) {
		String[] path2 = new String[path.length+1];
		for (int i = 0; i < path.length; i++) {
			path2[i] = path[i];
		}
		for (int i = 0; i < node.getChildren().size(); i++) {
			TFSNode newNode = node.getChildren().get(i);
			if (!newNode.getIsFile()) {
				path2[path2.length-1] = newNode.getName();
				recursiveCreateFile(path2, newNode, true, num);
			}
		}
		for (int i = 0; i < num; i++) {
			int j = i+1;
			String name = "File" + j;
			createFileNoID(path,name,true,true);
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param write
	 * @return
	 */
	public boolean recursiveDelete(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("IX"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error no directory exists.");
			client.error();
			return false;
		}
		else if (file.getIsFile()) {
			removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("IX"));
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
				writeLogEntry("recursiveDelete",path,null,-1);
			String[] path2 = new String[path.length-1];
			for (int i = 0; i < path2.length; i++) {
				path2[i] = path[i];
			}
			removeLocks(path2,0,root,new NodeLock("IX"),new NodeLock("IX"));
			client.complete();
			return true;
		}
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param offset
	 * @param bytes
	 * @return
	 */
	public boolean seekAndWrite(String[] path, int offset, byte[] bytes) {
		TFSNode file = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return false;
		}
		else if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			client.error();
			return false;
		}
		else {
			long id = file.getId();
			try {
				File jfile = new File("local/" + id);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				f.seek((long)offset);
				f.write(bytes);
				f.close();
				removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("X"));
				client.complete();
				return true;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			client.error();
			return false;
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
		TFSNode file = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return false;
		}
		else if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			client.error();
			return false;
		}
		else {
			long id = file.getId();
			try {
				File jfile = new File("local/" + id);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				f.seek(0);
				f.skipBytes((int)f.length());
				int length = bytes.length;
				f.writeInt(length);
				f.write(bytes);
				removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("X"));
				client.complete();
				return true;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		client.error();
		return false;
		}
	}
	
	/**
	 * 
	 * @param path
	 * @param bytes
	 * @return
	 */
	public boolean appendToFileNoSize(String[] path, byte[] bytes) {
		TFSNode file = searchTree(path,0,root,new NodeLock("IX"),new NodeLock("X"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return false;
		}
		else if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			client.error();
			return false;
		}
		else {
			long id = file.getId();
			try {
				File jfile = new File("local/" + id);
				RandomAccessFile f = new RandomAccessFile(jfile,"rw");
				f.seek(0);
				f.skipBytes((int)f.length());
				f.write(bytes);
				removeLocks(path,0,root,new NodeLock("IX"),new NodeLock("X"));
				client.complete();
				return true;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		client.error();
		return false;
		}
	}
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	public byte[] readFileNoSize(String[] path) {
		TFSNode file = searchTree(path,0,root,new NodeLock("IS"),new NodeLock("S"),true);
		synchronized(file) {
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return null;
		}
		else if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			client.error();
			return null;
		}
		else {
			long id = file.getId();
			try {
				File jfile = new File("local/" + id);
				RandomAccessFile f = new RandomAccessFile(jfile,"r");
				byte[] b = new byte[(int)f.length()];
				f.read(b);
				removeLocks(path,0,root,new NodeLock("IS"),new NodeLock("S"));
				client.complete();
				return b;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		client.error();
		return null;
		}
	}
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	public int countFilesInNode(String[] path) {
		TFSNode file = searchTree(path,0,root,new NodeLock("IS"),new NodeLock("S"),true);
		synchronized(file) {
		int count = 0;
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return -1;
		}
		else if (!file.getIsFile()) {
			System.out.println("Master: Error, end of path is not a file.");
			client.error();
			return -1;
		}
		else {
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
		removeLocks(path,0,root,new NodeLock("IS"),new NodeLock("S"));
		client.complete();
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
				System.out.println("File: " + node.getChildren().get(i).getName() + " (Locks: " + node.getChildren().get(i).printLock() + ")");
			}
			else {
				printTree(node.getChildren().get(i));
			}
		}
		if(node.getIsFile()) {
			System.out.println("File: " + node.getName() + " (Locks: " + node.printLock() + ")");
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
	public void writeLogEntry(String command, String[] path, String name, long id) {
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
					createFileWithID(path,name,false,id);
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
/*
	private void parseMessage(TFSMessage m){
		//check the parameters of m, figure out the corresponding method to call for that
		//those methods should finish by sending out the message and resetting the outgoingMessage 
		outgoingMessage.setDestination(m.getSource());
		switch (m.getMessageType()){
			case HANDSHAKE:
				System.out.println("Received handshake from " + m.getSourceType().toString() + " " + m.getSource());
				break;
			case CREATEDIRECTORY:
				System.out.println("Creating directory per request from " + m.getSource());
				createDirectory(m.getPath(),m.getFileName(),true);
				break;
			default:
				System.out.println("Invalid message");
				break;
		}
	}
	private TFSMessage resetMessage(TFSMessage m){
		//change all parameters besides messageSource and sourceType to null types 
		return m;
	}
	private ArrayList<TFSMessage> listenForTraffic(ArrayList<TFSMessage> q) throws ClassNotFoundException{
		try (
			ServerSocket serverSocket =
                new ServerSocket(portNumber);
            Socket clientSocket = serverSocket.accept();     
            ObjectOutputStream out =
                new ObjectOutputStream(clientSocket.getOutputStream()); //To send messages, probably not necessary here
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()); //Receive messages from the client
        ) {
			TFSMessage incomingMessage = new TFSMessage(); //create a new MessageObject, I think we should have the constructor set everything to null when it's initialized
			incomingMessage.receiveMessage(in); //call readObject 
			if (incomingMessage.getMessageType() != TFSMessage.mType.NONE){ //if we received data
				System.out.println("Received a message");
				q.add(incomingMessage);
			} 
			//Might make more sense to have an outgoingMessages Queue, and to send the Outgoing message with the proper flag set right after you read
			TFSMessage current = outgoingMessages.remove(0);
			if (current.getMessageType() != TFSMessage.mType.NONE)
				current.sendMessage(out);
			else 
				outgoingMessages.add(current);
			
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                + portNumber + " or listening for a connection");
            e.printStackTrace();
        } catch (ClassNotFoundException e){
		System.out.println("error");
	}
	return q;
	}
	*/
	/*If we want to send separately of listening, we'll need a new port*/
	/*
	private void sendTraffic(TFSMessage current){
		try (
            Socket messageSocket = new Socket(current.getDestination(), portNumber);
            ObjectOutputStream out =
                new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			System.out.println("Sending response to " + current.getDestination());
			current.sendMessage(out);
        } catch (UnknownHostException e) {
            System.err.println("Error: Don't know about host " + current.getDestination());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: Couldn't get I/O for the connection to " + current.getDestination());
            System.exit(1);
        } 
	}
	*/
}