import java.io.*;
import java.util.ArrayList;
public class TFSMessage implements Serializable{
	//Data fields for the message go here
	private String messageSource; // IP of the sender
	//private boolean hasInfo = false; // whether or not the message has been altered
	public enum Type {MASTER, CLIENT, CHUNK};
	private Type sourceType;
	
	ArrayList<String> path;
	String fileName;
	byte[] raw_data;
	int recursiveDeleteNum;
	int seekOffset;
	String errorMessage;
	public enum mType{CREATEFILE,CREATEDIRECTORY,DELETEFILE,DELETEDIRECTORY,HEARTBEAT,HEARTBEATRESPONSE,RECURSIVECREATE,SEEK,SIZEDAPPEND,APPEND,READFILE,COUNTFILES,ERROR,CREATEREPLICA,NONE};
	private mType messageType;
	
	public TFSMessage(){
		/*Null constructor for receiving messages*/
		messageSource = null;
		sourceType = null;
		messageType = mType.NONE;
	}
	public TFSMessage(String senderName, Type senderType){
		/*Basic constructor to set up attributes of the message for the sender*/
		messageSource = senderName;
		sourceType = senderType;
	}
/*Public methods for network behavior*/
	public void sendMessage(ObjectOutputStream o) throws IOException{ //public call to send this message out
		writeObject(o);
	}
	public void receiveMessage(ObjectInputStream o) throws IOException, ClassNotFoundException{ //public call to load new info to this message
		readObject(o);
	}
/*Private methods for network behavior (exist due to Serializable nature of TFSMessage)*/
	private void writeObject(ObjectOutputStream out) throws IOException{
		//the ObjectOutputStream is sending the fields as either writeObject or writeInt, just preserver the order
		out.writeObject(messageSource);
		out.writeObject(sourceType);
		out.writeObject(messageType);
	}
	private void readObject(ObjectInputStream in)throws IOException, ClassNotFoundException{
		//following the order from writeObject, just load things into variables
		messageSource = (String)in.readObject();
		sourceType = (Type)in.readObject();
		messageType = (mType)in.readObject();
	}
	private void readObjectNoData() throws ObjectStreamException{
		//this is just to say that something went wrong etc. 
	}
/*Getters & Setters*/
	public void setName(String s){ //Setter for the source IP
		messageSource = s;
	}
	public String getName(){ //Getter for the source IP
		return messageSource;
	}
	public Type getSourceType(){ //Getter for the Type
		return sourceType;
	}
	public mType getMessageType(){
		return messageType;
	}
	public void setMessageType(mType m){
		messageType = m;
	}
	
}