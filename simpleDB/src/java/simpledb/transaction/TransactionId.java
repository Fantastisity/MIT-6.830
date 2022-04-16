package simpledb.transaction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    static final AtomicLong counter = new AtomicLong(0);
    final long myid;
    private final long startTime;

    public TransactionId() {
        myid = counter.getAndIncrement();
        startTime = System.currentTimeMillis();
    }

    public long getId() {
        return myid;
    }
    
    public long getStartTime() {
        return startTime;
    }

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionId other = (TransactionId) obj;
        return myid == other.myid;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (myid ^ (myid >>> 32));
		return result;
	}
}
