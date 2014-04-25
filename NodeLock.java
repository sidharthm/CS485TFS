public class NodeLock {
	public enum Lock { 
		S(2), X(3), IS(0), IX(1);
		
		final int value;
		Lock(int v) {
			value = v;
		}
		public int getValue() {
			int v = value;
			return v;
		}
	}
	
	Lock lock;
	
	public NodeLock(String l) {
		//if (l.equals("NONE"))
			//lock = Lock.NONE;
		/*else*/ if (l.equals("S"))
			lock = Lock.S;
		else if (l.equals("X"))
			lock = Lock.X;
		else if (l.equals("IS"))
			lock = Lock.IS;
		else if (l.equals("IX"))
			lock = Lock.IX;
		//else if (l.equals("SIX"))
			//lock = Lock.SIX;
		else
			System.out.println("Invalid lock type");
	}
	
	/*
	public void setLock(NodeLock l) {
		synchronized(this) {
			lock = l.getLock();
		}
	}
	*/
	
	public Lock getLock() {
		Lock l = lock;
		return l;
	}
	
	public String toString() {
		synchronized(this) {
			if (lock == Lock.S) {
				return "S";
			}
			else if (lock == Lock.X) {
				return "X";
			}
			else if (lock == Lock.IS) {
				return "IS";
			}
			else if (lock == Lock.IX) {
				return "IX";
			}
			else {
				System.out.println("Invalid lock type");
				return null;
			}
		}
	}

	public boolean checklock(NodeLock l) {
		synchronized(this) {
			if (l.getLock() == Lock.S) {
				//if (lock == Lock.NONE)
					//return true;
				/*else*/ if (lock == Lock.S)
					return true;
				else if (lock == Lock.X)
					return false;
				else if (lock == Lock.IS)
					return true;
				else if (lock == Lock.IX)
					return false;
				//else if (lock == Lock.SIX)
					//return false;
				else
					return false;
			}
			else if (l.getLock() == Lock.X) {
				//if (lock == Lock.NONE)
					//return true;
				/*else*/ if (lock == Lock.S)
					return false;
				else if (lock == Lock.X)
					return false;
				else if (lock == Lock.IS)
					return false;
				else if (lock == Lock.IX)
					return false;
				//else if (lock == Lock.SIX)
					//return false;
				else
					return false;
			}
			else if (l.getLock() == Lock.IS) {
				//if (lock == Lock.NONE)
					//return true;
				/*else*/ if (lock == Lock.S)
					return true;
				else if (lock == Lock.X)
					return false;
				else if (lock == Lock.IS)
					return true;
				else if (lock == Lock.IX)
					return true;
				//else if (lock == Lock.SIX)
					//return true;
				else
					return false;
			}
			else if (l.getLock() == Lock.IX) {
				//if (lock == Lock.NONE)
					//return true;
				/*else*/ if (lock == Lock.S)
					return false;
				else if (lock == Lock.X)
					return false;
				else if (lock == Lock.IS)
					return true;
				else if (lock == Lock.IX)
					return true;
				//else if (lock == Lock.SIX)
					//return false;
				else
					return false;
			}
			/*
			else if (l.getLock() == Lock.SIX) {
				//if (lock == Lock.NONE)
					//return true;
			 */
				/*else*/ /*if (lock == Lock.S)
					return false;
				else if (lock == Lock.X)
					return false;
				else if (lock == Lock.IS)
					return true;
				else if (lock == Lock.IX)
					return false;
				else if (lock == Lock.SIX)
					return false;
				else
					return false;
			}
			*/
			else {
				System.out.println("Illegal lock");
				return false;
			}
		}
	}
}