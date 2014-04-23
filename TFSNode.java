import java.util.ArrayList;

public class TFSNode {

	TFSNode parent;
	ArrayList<TFSNode> children;
	boolean isFile; //true if file, false if directory
	String name;
	long id;

	public TFSNode(boolean file, TFSNode p, long i, String n) {
		parent = p;
		isFile = file;
		children = new ArrayList<TFSNode>();
		name = n;
		id = i;
	}

	public ArrayList<TFSNode> getChildren() {
		return children;
	}

	public String getName() {
		return name;
	}


	public boolean getIsFile() {
		return isFile;
	}

	public TFSNode getParent() {
		return parent;
	}

	public long getId() {
		return id;
	}

}