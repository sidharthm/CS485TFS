import java.io.*;
public class TFSMessage implements Serializable{
	//Data fields for the message go here
	private String messageSource;
	public enum Type {MASTER, CLIENT, CHUNK};
	private Type sourceType;

	
	public TFSMessage(){
		messageSource = null;
		sourceType = null;
	}
	public TFSMessage(String senderName, Type senderType){
		messageSource = senderName;
		sourceType = senderType;
	}

	public void setName(String s){ //Setter
		messageSource = s;
	}
	public String getName(){ //Getter
		return messageSource;
	}
	public Type getType(){
		return sourceType;
	}
	public void sendMessage(ObjectOutputStream o) throws IOException{
		writeObject(o);
	}
	public void receiveMessage(ObjectInputStream o) throws IOException, ClassNotFoundException{
		readObject(o);
	}
	private void writeObject(ObjectOutputStream out) throws IOException{
		//the ObjectOutputStream is sending the fields as either writeObject or writeInt, just preserver the order
		out.writeObject(messageSource);
		out.writeObject(sourceType);
	}
	 private void readObject(ObjectInputStream in)throws IOException, ClassNotFoundException{
		//following the order from writeObject, just load things into variables
		messageSource = (String)in.readObject();
		sourceType = (Type)in.readObject();
	}
	 private void readObjectNoData() throws ObjectStreamException{
		//this is just to say that something went wrong etc. 
	}
}