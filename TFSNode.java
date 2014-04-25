import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TFSNode {

	TFSNode parent;
	List<TFSNode> children;
	boolean isFile; //true if file, false if directory
	String name;
	long id;
	List<NodeLock> locks;

	public TFSNode(boolean file, TFSNode p, long i, String n) {
		parent = p;
		isFile = file;
		children = Collections.synchronizedList(new ArrayList<TFSNode>());
		name = n;
		id = i;
		locks = Collections.synchronizedList(new ArrayList<NodeLock>());
	}
	
	public List<TFSNode> getChildren() {
		return children;
	}

	public String getName() {
		String n = name;
		return n;
	}


	public boolean getIsFile() {
		boolean f = isFile;
		return f;
	}

	public TFSNode getParent() {
		TFSNode p = parent;
		return p;
	}

	public long getId() {
		return id;
	}
	
	public List<NodeLock> getLock() {
		return locks;
	}
	
	public String printLock() {
		synchronized(this) {
			String lockvalue = "";
			if (locks.size() == 0) {
				lockvalue = "NONE";
			}
			else {
				for (int i = 0; i < locks.size(); i++) {
					lockvalue += locks.get(i).toString();
					lockvalue += " ";
				}
			}
			return lockvalue;
		}
	}
	
	public boolean checkLocks(NodeLock l) {
		synchronized(this) {
			for (int i = 0; i < locks.size(); i++) {
				if (!locks.get(i).checklock(l))
					return false;
			}
			return true;
		}
	}
	
	public void removeLock(NodeLock l) {
		synchronized(this) {
			for (int i = 0; i < locks.size(); i++) {
				NodeLock lock = locks.get(i);
				if (lock.getLock().getValue() == l.getLock().getValue()) {
					locks.remove(i);
					break;
				}
			}
		}
	}

}