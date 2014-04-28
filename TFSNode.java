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
	List<String> replicas;
	int desiredReplicas;

	public TFSNode(boolean file, TFSNode p, long i, String n, int d) {
		parent = p;
		isFile = file;
		children = Collections.synchronizedList(new ArrayList<TFSNode>());
		name = n;
		id = i;
		locks = Collections.synchronizedList(new ArrayList<NodeLock>());
		replicas = Collections.synchronizedList(new ArrayList<String>());
		desiredReplicas = d;
	}
	
	public List<TFSNode> getChildren() {
		return children;
	}
	
	public List<String> getReplicas() {
		return replicas;
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
		synchronized(locks) {
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
		synchronized(locks) {
			for (int i = 0; i < locks.size(); i++) {
				if (!locks.get(i).checklock(l))
					return false;
			}
			return true;
		}
	}
	
	public void removeLock(NodeLock l) {
		synchronized(locks) {
			for (int i = 0; i < locks.size(); i++) {
				NodeLock lock = locks.get(i);
				if (lock.getLock().getValue() == l.getLock().getValue()) {
					locks.remove(i);
					break;
				}
			}
		}
	}
	
	public String printReplicas() {
		synchronized(replicas) {
			String replicaString = "";
			if (replicas.size() == 0) {
				replicaString = "NONE";
			}
			else {
				for (int i = 0; i < replicas.size(); i++) {
					replicaString += replicas.get(i).toString();
					replicaString += " ";
				}
			}
			return replicaString;
		}
	}
	
	public int getDesiredReplicas() {
		int d = desiredReplicas;
		return d;
	}

}