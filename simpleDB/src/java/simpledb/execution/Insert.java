package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private OpIterator child;
    private final TupleDesc td;
    private final int tableId;
    private boolean called = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t; this.child = child; this.tableId = tableId;
        td = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(td)) throw new DbException("TupleDescs differ");
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Val"});
    }

    public void open() throws DbException, TransactionAbortedException {
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) return null;
        Tuple tp = new Tuple(getTupleDesc());
        int cnt = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(t, tableId, child.next());
                ++cnt;
            } catch (Exception e) {}
        }
        tp.setField(0, new IntField(cnt));
        called = true;
        return tp;
    }

    @Override
    public OpIterator[] getChildren() {
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    }
}
