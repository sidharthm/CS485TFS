import java.io.*;
import java.util.ArrayList;
public class TFSMessage implements Serializable{
	//Data fields for the message go here
	private String messageSource; // IP of the sender
	private String messageDestination; // IP this is going to (only used by the master)
	//private boolean hasInfo = false; // whether or not the message has raw data REMOVE -- better to do a switch on the Type and send/read accordingly
	public enum Type {MASTER, CLIENT, CHUNK};
	private Type sourceType;

	String[] path;
	String fileName;
	byte[] raw_data;
	int recursiveCreateNum;
	int seekOffset;
	String errorMessage;
	public enum mType{CREATEFILE,CREATEDIRECTORY,DELETE,HANDSHAKE,HEARTBEAT,HEARTBEATRESPONSE,RECURSIVECREATE,SEEK,SIZEDAPPEND,APPEND,READFILE,COUNTFILES,SUCCESS,ERROR,CREATEREPLICA,NONE,INITIALIZE,PRINT};
	private mType messageType;

	public TFSMessage(){
		/*Null constructor for receiving messages*/
		messageSource = null;
		sourceType = null;
		messageType = mType.NONE;
	}
	/**
	 * Constructor to set up attributes of the message for the sender
	 */
	public TFSMessage(String senderName, Type senderType){
		/*Basic constructor to set up attributes of the message for the sender*/
		messageSource = senderName;
		sourceType = senderType;
	}
/*Public methods for network behavior*/
	/**
	 * Send method for network behavior
	 */
	public void sendMessage(ObjectOutputStream o) throws IOException{ //public call to send this message out
		writeObject(o);
	}
	/**
	 * Receive message for network behavior
	 */
	public void receiveMessage(ObjectInputStream o) throws IOException, ClassNotFoundException{ //public call to load new info to this message
		readObject(o);
	}
/*Private methods for network behavior (exist due to Serializable nature of TFSMessage)*/
	/**
	 * Private methods for network behavior (exist due to Serializable nature of TFSMessage
	 * @param Object Output Stream
	 */
	private void writeObject(ObjectOutputStream out) throws IOException{
		//the ObjectOutputStream is sending the fields as either writeObject or writeInt, just preserver the order
		out.writeObject(messageSource);
		out.writeObject(sourceType);
		out.writeObject(messageType);
		switch (messageType){
			case CREATEDIRECTORY:
			case CREATEFILE:
				out.writeObject(path);
				out.writeObject(fileName);
				break;
			case DELETE:
				out.writeObject(path);
				break;
			case SEEK:
				out.writeObject(path);
				out.writeInt(seekOffset);
				out.writeObject(raw_data);
				break;
			case SIZEDAPPEND:
				//Where is append with size?
			case APPEND:
				out.writeObject(path);
				out.writeObject(raw_data);
				break;
			case READFILE:
				out.writeObject(path);
				out.writeObject(fileName);
				out.writeObject(raw_data);
				break;
			case COUNTFILES:
			case RECURSIVECREATE:
				break;
			/*SUCCESS,ERROR,CREATEREPLICA*/
				//Check Test 3
			case CREATEREPLICA:
				break;
			case SUCCESS:
			case ERROR:
				break;
		}
	}
	/**
	 * Reading the objects based on message types
	 * @param ObjectInput Stream
	 */
	private void readObject(ObjectInputStream in)throws IOException, ClassNotFoundException{
		//following the order from writeObject, just load things into variables
		messageSource = (String)in.readObject();
		sourceType = (Type)in.readObject();
		messageType = (mType)in.readObject();
		
		switch (messageType){
			case CREATEDIRECTORY:
			case CREATEFILE:
				path = (String[])in.readObject();
				fileName = (String)in.readObject();
				break;
			case DELETE:
				path = (String[])in.readObject();
				break;
			case SEEK:
				path = (String[])in.readObject();
				seekOffset = (int)in.readObject();
				raw_data = (byte[])in.readObject();
				break;
			case SIZEDAPPEND:
				//Where is append with size?
			case APPEND:
				path = (String[])in.readObject();
				raw_data = (byte[])in.readObject();
				break;
			case READFILE:
				path = (String[])in.readObject();
				fileName = (String)in.readObject();
				raw_data = (byte[])in.readObject();
				break;
			case COUNTFILES:
			case RECURSIVECREATE:
				break;
			case CREATEREPLICA:
				break;
			case SUCCESS:
			case ERROR:
				break;
		}
	}
	private void readObjectNoData() throws ObjectStreamException{
		//this is just to say that something went wrong etc. 
	}
/*Getters & Setters*/
	/**
	 * Setting the source (IP) for the message
	 */
	public void setSource(String s){ //Setter for the source IP
		messageSource = s;
	}
	/**
	 * Getting the source (IP) for the message
	 */
	public String getSource(){ //Getter for the source IP
		return messageSource;
	}
	/**
	 * Setting the message destination
	 * @param String destination
	 */
	public void setDestination(String d){
		messageDestination = d;
	}
	/**
	 * Getting the message destination
	 * @param String destination
	 */
	public String getDestination(){
		return messageDestination;
	}
	/**
	 * Getter for the source type
	 */
	public Type getSourceType(){ //Getter for the Type
		return sourceType;
	}
	/**
	 * Setter for the source type
	 */
	public mType getMessageType(){
		return messageType;
	}
	/**
	 * Setter for the message type
	 * @param enum mType
	 */
	public void setMessageType(mType m){
		messageType = m;
	}
	/**
	 * Setter for the path
	 * @param String[] path
	 */
	public void setPath(String[] p){
		path = p;
	}
	/**
	 * Getter for the path
	 * @param String path
	 */
	public String[] getPath(){
		return path;
	}
	/**
	 * Setter for the file name
	 * @param String filename
	 */
	public void setFileName(String s){
		fileName = s;
	}
	/**
	 * Getter for the file name
	 */
	public String getFileName(){
		return fileName;
	}
	/**
	 * Setter for the offset for seek
	 * @param integer offset
	 */
	public void setOffset(int o){
		seekOffset = o;
	}
	/**
	 * Getter for the offset for seek
	 */
	public int getOffset(){
		return seekOffset;
	}
	/**
	 * Setter for the bytes of a file
	 * @param byte[] bytes
	 */
	public void setBytes(byte[] a){
		raw_data = a;
	}
	/**
	 * Getter for the bytes
	 */
	public byte[] getBytes(){
		return raw_data;
	}
	/*
	public boolean getDataState(){
		return hasInfo;
	}
	public void setDataState(boolean b){
		hasInfo = b;
	}
	*/
	/**
	 * Getter for file num for recursive create
	 */
	public int getFileNum() {
		return recursiveCreateNum;
	}
	/**
	 * Setter for file num for recursive create
	 */
	public void setFileNum(int r) {
		recursiveCreateNum = r;
	}
}