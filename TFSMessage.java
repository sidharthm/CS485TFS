import java.io.*;
public class TFSMessage implements Serializable{
	//Data fields for the message go here
	private String messageSource; // IP of the sender
	private boolean hasInfo = false; // whether or not the message has been altered
	public enum Type {MASTER, CLIENT, CHUNK};
	private Type sourceType;

	
	public TFSMessage(){
		/*Null constructor for receiving messages*/
		messageSource = null;
		sourceType = null;
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
		out.writeBoolean(hasInfo);
	}
	private void readObject(ObjectInputStream in)throws IOException, ClassNotFoundException{
		//following the order from writeObject, just load things into variables
		messageSource = (String)in.readObject();
		sourceType = (Type)in.readObject();
		hasInfo = in.readBoolean();
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
	public Type getType(){ //Getter for the Type
		return sourceType;
	}
	public void setInfo(){ //Flag for significant changes to the message
		hasInfo = true;
	}
	public boolean hasData(){ //Check flag for significant changes to the message
		return hasInfo;
	}
	
}