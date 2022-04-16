package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.*;
import simpledb.common.Type;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private int afield, gfield;
    private OpIterator child;
    private boolean f = false;
    private Aggregator.Op aop;
    private Aggregator aggr;
    private OpIterator it = null;
    private Type gbfieldtype, afieldtype;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        gbfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);
        afieldtype = child.getTupleDesc().getFieldType(afield);
        if (afieldtype == Type.INT_TYPE) 
            aggr = new IntegerAggregator(gfield, gbfieldtype, afield, aop); 
        else aggr = new StringAggregator(gfield, gbfieldtype, afield, aop);
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return gbfieldtype == null ? null : "groupVal";
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return "Val";
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) aggr.mergeTupleIntoGroup(child.next());
        it = aggr.iterator();
        it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        return it.hasNext() ? it.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind(); it.rewind();
    }

    public TupleDesc getTupleDesc() {
        TupleDesc td;
        if (aop != Aggregator.Op.SUM_COUNT) 
            td = new TupleDesc(new Type[]{gbfieldtype, afieldtype}, new String[]{groupFieldName(), "Val"});
        else
            td = new TupleDesc(new Type[]{gbfieldtype, afieldtype, Type.INT_TYPE}, 
                                       new String[]{groupFieldName(), "sumVal", "countVal"});
        return td;
    }

    public void close() {
        child.close();
        super.close();
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
