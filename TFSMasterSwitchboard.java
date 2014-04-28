import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;


public class TFSMasterSwitchboard implements Runnable{
	
	TFSMaster mPrime;
	Object mPrimeLock;
	TFSMaster m2;
	Object m2Lock;
	boolean initialized;
	private String myName;//This contains the server's IP
	private int portNumber = 4444;//This details the port to be used for trafficking of information
	private Socket serverSocket; //this socket is used to communicate with the client
	private List<TFSMessage> outgoingMessages; //This message is used to convey information to other entities in the system
	private TFSMessage heartbeatMessage;//This message is used to ensure chunk servers are still operational
	private List<TFSMessage> incomingMessages; // This Queue will store any incoming messages
	private TFSNode root;
	private ArrayList<String> chunkServers;
	private ArrayList<String> responses;
	Timer timer = new Timer();
	Timer timer2 = new Timer();
	
	public TFSMasterSwitchboard() {
		/*TEMPORARY: Master reads its own data from the config file so it has its own IP*/
		try {
			Scanner inFile = new Scanner(new File("config.txt"));
			while (inFile.hasNext()){
				String input = inFile.next();
				if (input.equals("MASTER"))
					myName = inFile.next();
			}
		} catch (FileNotFoundException e){
			System.err.println("Error: Configuration file not found");
			System.exit(1);
		}
		outgoingMessages = Collections.synchronizedList(new ArrayList<TFSMessage>());
		//outgoingMessage = new TFSMessage(myName,TFSMessage.Type.MASTER);
		heartbeatMessage = new TFSMessage(myName,TFSMessage.Type.MASTER);
		incomingMessages = Collections.synchronizedList(new ArrayList<TFSMessage>());
		System.out.println("My ip is " + myName);
		root = new TFSNode(false,null,-1,"root",0);
		mPrime = new TFSMaster(this,mPrimeLock);
		m2 = new TFSMaster(this,m2Lock);
		mPrime.initializeStructure();
		timer.scheduleAtFixedRate(new TimerTask() {
	        public void run() {
	            //TODO send heartbeat
	        	responses.clear();
	        	timer2.schedule(new TimerTask() {
	    	        public void run() {
	    	           for (int i = 0; i < chunkServers.size(); i++) {
	    	        	   if (responses.indexOf(chunkServers.get(i)) == -1) {
	    	        		   chunkServers.remove(i);
	    	        	   }
	    	           }
	    	        }
	    	    }, 0, 30000);
	        }
	    }, 0, 60000);
		
		Thread thread2 = new Thread(mPrime);
		Thread thread3 = new Thread(m2);
		thread2.start();
		thread3.start();
	}
	
	public TFSNode getRoot() {
		return root;
	}
	
	public List<TFSMessage> getOutgoingMessages() {
		return outgoingMessages;
	}
	
	public ArrayList<String> getChunkServers() {
		return chunkServers;
	}
	
	public void addOutgoingMessage(TFSMessage m) {
		synchronized(outgoingMessages) {
			outgoingMessages.add(m);
		}
	}
	
	public void run() {
		while(true) {
			scheduler();
		}
	}
	
	private void scheduler() {
		try {
			if (!outgoingMessages.isEmpty()) {
				sendTraffic(outgoingMessages.remove(0));
			}
			incomingMessages = listenForTraffic(incomingMessages); //update incomingMessages as required
			if (!incomingMessages.isEmpty()){ //If we have messages that need to be processed
			 	parseMessage(incomingMessages.remove(0)); // identify what needs to be done based on the parameters of the first message, and respond
			}
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
	
	private void sendTraffic(TFSMessage current){
		try (
            Socket messageSocket = new Socket(current.getDestination(), portNumber);
            ObjectOutputStream out =
                new ObjectOutputStream(messageSocket.getOutputStream()); //allows us to write objects over the socket
        ) {
			current.sendMessage(out);
        } catch (UnknownHostException e) {
            System.err.println("Error: Don't know about host " + current.getDestination());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error: Couldn't get I/O for the connection to " + current.getDestination());
            System.exit(1);
        } 
	}

	
	private TFSMessage resetMessage(TFSMessage m){
		//change all parameters besides messageSource and sourceType to null types 
		return m;
	}
	private List<TFSMessage> listenForTraffic(List<TFSMessage> q) throws ClassNotFoundException{
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
	
	private void parseMessage(TFSMessage m){
		//check the parameters of m, figure out the corresponding method to call for that
		//those methods should finish by sending out the message and resetting the outgoingMessage 
		TFSMessage.mType type = m.getMessageType();
		if (type == TFSMessage.mType.HANDSHAKE){
			chunkServers.add(m.getSource());
		}
		else if (type == TFSMessage.mType.HEARTBEATRESPONSE) {
			responses.add(m.getSource());
		}
		else {
			mPrime.addMessage(m);
		}
	}
	
	public static void main(String[] args) {
		TFSMasterSwitchboard testBoard = new TFSMasterSwitchboard();
		Thread thread = new Thread(testBoard);
		thread.start();
	}
}
