import java.util.ArrayList;
import java.io.*;
import java.math.BigInteger;

public class TFSMaster {
	
	TFSNode root;
	TFSClient client;
	File logfile;

	public TFSMaster(TFSClient c) {
		root = new TFSNode(false,null,-1,"root");
		client = c;
		File file = new File("local/");
		file.mkdirs();
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
	
	public TFSNode getRoot() {
		return root;
	}
	
	public TFSNode searchTree(String[] path, int index, TFSNode node) {
		if (index == path.length) {
			return node;
		}
		else {
			TFSNode newNode = null;
			for (int i = 0; i < node.getChildren().size(); i++) {
				if (node.getChildren().get(i).getName().equals(path[index])) {
					newNode = node.getChildren().get(i);
					break;
				}
			}
			if (newNode != null)
				return searchTree(path,index+1,newNode);
			else {
				//System.out.println("Path does not exist");
				return null;
			}
		}
	}

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
	
	public boolean createFileWithID(String[] path, String name, boolean write, long oldID) {
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
				long id = oldID;
				TFSNode newNode = new TFSNode(true,directory,id,name);
				directory.getChildren().add(newNode);
				try {
					File file = new File("local/" + id);
					file.createNewFile();
					if (write)
						writeLogEntry("createFileID",path,name,id);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			return false;
		}
	}
	
	public boolean createFileNoID(String[] path, String name, boolean write, boolean error) {
		TFSNode directory = searchTree(path,0,root);
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
	
	public boolean createDirectory(String[] path, String name, boolean write) {
		TFSNode directory = searchTree(path,0,root);
		if (directory == null) {
			System.out.println("Master: Error, no directory exists.");
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
				client.complete();
				return true;
			}
			client.error();
			return false;
		}
	}

	public boolean deleteFile(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,root);
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
			return true;
		}
	}

	public boolean deleteFileNode(TFSNode file) {
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

	public void deleteDirectoryNode(TFSNode directory) {
		directory.getParent().getChildren().remove(directory);
		directory = null;
	}

	public void recursiveDeleteFromNode(TFSNode node) {
		for (int i = 0; i < node.getChildren().size(); i++) {
			if (node.getChildren().get(i).getIsFile()) {
				deleteFileNode(node.getChildren().get(i));
			}
			else if (node.getChildren().get(i).getChildren().size() == 0) {
				deleteDirectoryNode(node.getChildren().get(i));
			}
			else {
				recursiveDeleteFromNode(node.getChildren().get(i));
			}
		}
		if(node.getIsFile()) {
			deleteFileNode(node);
		}
		else {
			deleteDirectoryNode(node);
		}
	}
	
	public boolean recursiveCreateFileInitial(String[] path, boolean write, int num) {
		TFSNode file = searchTree(path,0,root);
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
	
	public void recursiveCreateFile(String[] path, TFSNode node, boolean write, int num) {
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
	
	public boolean recursiveDelete(String[] path, boolean write) {
		TFSNode file = searchTree(path,0,root);
		if (file == null) {
			System.out.println("Master: Error, no directory exists.");
			client.error();
			return false;
		}
		else if (file.getIsFile()) {
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
			recursiveDeleteFromNode(file);
			if (write)
				writeLogEntry("recursiveDelete",path,null,-1);
			client.complete();
			return true;
		}
	}
	
	public boolean seek(String[] path, int offset, byte[] bytes) {
		TFSNode file = searchTree(path,0,root);
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
	
	public boolean appendToFileWithSize(String[] path, byte[] bytes) {
		TFSNode file = searchTree(path,0,root);
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
	public boolean appendToFileNoSize(String[] path, byte[] bytes) {
		TFSNode file = searchTree(path,0,root);
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
	
	public byte[] readFileNoSize(String[] path) {
		TFSNode file = searchTree(path,0,root);
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
	
	public int countFilesInNode(String[] path) {
		TFSNode file = searchTree(path,0,root);
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
		client.complete();
		return count;
	}
	
	public void printTree(TFSNode node) {
		for (int i = 0; i < node.getChildren().size(); i++) {
			if (node.getChildren().get(i).getIsFile()) {
				System.out.println("File: " + node.getChildren().get(i).getName());
			}
			else if (node.getChildren().get(i).getChildren().size() == 0) {
				System.out.println("Directory: " + node.getChildren().get(i).getName());
			}
			else {
				printTree(node.getChildren().get(i));
			}
		}
		if(node.getIsFile()) {
			System.out.println("File: " + node.getName());
		}
		else {
			System.out.println("Directory: " + node.getName());
		}
	}
	
	public void writeLogEntry(String command, String[] path, String name, long id) {
		FileWriter fstream;
		try {
			fstream = new FileWriter("log.txt",true);
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readLogFile() {
		try {
			BufferedReader in = new BufferedReader(new FileReader("log.txt"));
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
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}