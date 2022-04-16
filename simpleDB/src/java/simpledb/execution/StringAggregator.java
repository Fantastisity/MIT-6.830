package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import java.util.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    int gbfield, afield;
    Type gbfieldtype;
    Op what;
    TupleDesc td;
    Tuple tp;
    
    LinkedHashMap<Field, Integer> mp = new LinkedHashMap<>();
    Iterator<Map.Entry<Field, Integer>> it;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT) throw new IllegalArgumentException("only supports COUNT");
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tp = gbfield == -1 ? null : tup.getField(gbfield);
        mp.put(tp, mp.getOrDefault(tp, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new Operator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {
                super.open();
                it = mp.entrySet().iterator();
                td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "Val"});
            }
            
            @Override
            protected Tuple fetchNext() {
                if (!it.hasNext()) return null;
                Map.Entry<Field, Integer> ent = it.next();
                tp = new Tuple(td);
                
                tp.setField(0, ent.getKey());
                tp.setField(1, new IntField(ent.getValue().intValue()));
                
                return tp;
            }
            
            @Override
            public OpIterator[] getChildren() {
                return null;
            }
            
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close(); open();
            }
            
            @Override
            public void setChildren(OpIterator[] children) {
                
            }
            
            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }
         };
    }
}
