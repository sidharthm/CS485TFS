import java.io.*;
import java.util.ArrayList;
public class TFSMessage implements Serializable{
	//Data fields for the message go here
	private String messageSource; // IP of the sender
	private String messageDestination; // IP this is going to (only used by the master)
	private boolean hasInfo = false; // whether or not the message has raw data
	public enum Type {MASTER, CLIENT, CHUNK};
	private Type sourceType;
	
	String[] path;
	String fileName;
	byte[] raw_data;
	int recursiveDeleteNum;
	int seekOffset;
	String errorMessage;
	public enum mType{CREATEFILE,CREATEDIRECTORY,DELETE,HANDSHAKE,HEARTBEAT,HEARTBEATRESPONSE,RECURSIVECREATE,SEEK,SIZEDAPPEND,APPEND,READFILE,COUNTFILES,SUCCESS,ERROR,CREATEREPLICA,NONE};
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
		if (messageType != mType.HANDSHAKE){
			out.writeObject(path);
			out.writeObject(fileName);
			out.writeBoolean(hasInfo);
			if (hasInfo){
				out.writeInt(raw_data.length);
				for (int i = 0; i < raw_data.length; i++)
					out.writeByte(raw_data[i]);
			}
		}
		
	}
	private void readObject(ObjectInputStream in)throws IOException, ClassNotFoundException{
		//following the order from writeObject, just load things into variables
		messageSource = (String)in.readObject();
		sourceType = (Type)in.readObject();
		messageType = (mType)in.readObject();
		if (messageType != mType.HANDSHAKE) {
			path = (String[])in.readObject();
			fileName = (String)in.readObject();
			hasInfo = in.readBoolean();
			if (hasInfo){
				int length = in.readInt();
				raw_data = new byte[length];
				for (int i = 0; i < length; i++)
					raw_data[i] = in.readByte();
			}
		}
	}
	private void readObjectNoData() throws ObjectStreamException{
		//this is just to say that something went wrong etc. 
	}
/*Getters & Setters*/
	public void setSource(String s){ //Setter for the source IP
		messageSource = s;
	}
	public String getSource(){ //Getter for the source IP
		return messageSource;
	}
	public void setDestination(String d){
		messageDestination = d;
	}
	public String getDestination(){
		return messageDestination;
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
	public void setPath(String[] p){
		path = p;
	}
	public String[] getPath(){
		return path;
	}
	public void setFileName(String s){
		fileName = s;
	}
	public String getFileName(){
		return fileName;
	}
	public void setOffset(int o){
		seekOffset = o;
	}
	public int getOffset(){
		return seekOffset;
	}
	public void setBytes(byte[] a){
		raw_data = a;
	}
	public byte[] getBytes(){
		return raw_data;
	}
	public boolean getDataState(){
		return hasInfo;
	}
	public void setDataState(boolean b){
		hasInfo = b;
	}
	
}