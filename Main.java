import java.io.*;


public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TFSClient client;
		TFSMaster master;
		client = new TFSClient();
		master = new TFSMaster(client);
		client.setMaster(master);
		/*
		String[] path;
		path = new String[0];
		master.createFileNoID(path,"File1",true);
		master.printTree(master.getRoot());
		path = new String[1];
		path[0] = "File1";
		File file = new File("a.txt");
		try {
			RandomAccessFile f = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)f.length()];
			f.read(b);
			master.appendToFileWithSize(path,b);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(master.countFilesInNode(path));
		*/
	}

}
