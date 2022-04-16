package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private OpIterator child;
    private boolean called = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t; this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Val"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        called = false;
    }

    public void close() {
        super.close();
        child.close();
        called = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close(); open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) return null;
        Tuple tp = new Tuple(getTupleDesc());
        int cnt = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(t, child.next());
                ++cnt;
            } catch (Exception e) {}
        }
        tp.setField(0, new IntField(cnt));
        called = true;
        return tp;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
