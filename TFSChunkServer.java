import java.util.ArrayList;
import java.io.*;

public class TFSChunkServer {

	//Where in local are we storing all files - default location
	String location = "C:\Users\dbadmin\Documents\TFSFiles";
	//Relating handles and files
	Map<long, byte[]> mapHandleFile = new HashMap<long, byte[]>();
	//64-bit handles for files
	long nextAvailableHandle = 0;	
	int chunkServerID = 0;
	static int nextAvailableChunkServerID = 0;
	
	public TFSChunkServer() {
		chunkServerID = nextAvailableChunkServerID;
		//location = fileLocation;
		nextAvailableChunkServerID++;
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
        
        if (args.length != 1) {
            System.err.println("Usage: java EchoServer <port number>");
            System.exit(1);
        }
        
        int portNumber = Integer.parseInt(args[0]);
        
        try (
            ServerSocket serverSocket =
                new ServerSocket(Integer.parseInt(args[0]));
            Socket clientSocket = serverSocket.accept();     
            PrintWriter out =
                new PrintWriter(clientSocket.getOutputStream(), true); //Sends the String back to client to print, can change this to ObjectOutput to send messages
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()); //Receive messages from the client
        ) {
			int messageCount = 0; //Just a hack to keep it from closing the connection. Probably need to throw all this into the "run" method
			while (messageCount < 10){
				TFSMessage incomingMessage = new TFSMessage(); //create a new MessageObject, I think we should have the constructor set everything to null when it's initialized
				incomingMessage.setName("nothing"); //Just giving it a name value to make sure a change is observed
				incomingMessage.receive(in); //call readObject 
				String content = incomingMessage.getName() + " from Server"; //pull the changed name in our new object
				if (content != null){ //if we received data
					messageCount++; //increment the counter
					out.println(content); //send the data back to the client
				}
			}
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        }
		
		//Store files
		//I need local path on where to store it - my decision
		public void storeTest(byte[] b) {
			//HACK: STILL NEED TO KNOW WHERE TO STORE IT - HERE FIRST - how to know what to name file
			String newLocation = location + "testOne.jpg";
			try{
				File f = new File(newLocation);
				//File already exists
				if(f.exists()) {
					System.out.println("ChunkServer " + chunkServerID + ": Error, file already exists in local.");
				}
				else {
					//Opening local location and writing/adding file to it
					FileOutputStream fileOutputStream = new FileOutputStream(newLocation);
					fileOutputStream.write(b);
					fileOutputStream.close();
					
					//HACK: this is how master did it
						RandomAccessFile TFSFile = new RandomAccessFile(f,"rw");
						TFSFile.seek(0);
						TFSFile.skipBytes((int)TFSFile.length());
						TFSFile.write(bytes);
					//End of how master did it
					
					//Since adding file to local is successful need to add to map
					mapHandleFile.put(nextAvailableHandle, b);
					nextAvailableHandle++;
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//Retrieve Files
		public void retrieveTest(long fileHandle) {
			//What is the point of storing in local if you can do this??
			new byte[] b = map.get(fileHandle);
			
			//WEIRD: Since all I get is a handle this doesn't make sense
			File file = new File("C:\\one.jpg");
			//File does not exists
			if(!file.exists()) {
				System.out.println("ChunkServer " + chunkServerID + ": Error, file does not exist in local.");
			}
			else {
				RandomAccessFile f = new RandomAccessFile(file, "r");
				byte[] b = new byte[(int)f.length()];
				//f.read(b);
				
				//HACK: Here is how master did it.
					File jfile = new File("local/" + id);
					RandomAccessFile f = new RandomAccessFile(jfile,"r");
					byte[] b = new byte[(int)f.length()];
					f.read(b);	
				//End of how master did it
				
				//WAIT: Need to send the file to client
			}
		}
		
		//Append to files -  i will get what to append and where to append it - not sure if right arrangement
		//WAIT: are we allowed to append if file didn't exist?
		public void appendTest(byte[] b,long fileHandle) {
			
		}
			
			
			
			
	}
}
	
	
//Look for HACK AND WEIRD AND WAIT if checking for parts still not sure