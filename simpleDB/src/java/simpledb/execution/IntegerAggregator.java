package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import java.util.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield, cnt;
    private Type gbfieldtype;
    private Op what;
    private LinkedHashMap<Field, Map.Entry<Integer, Integer>> mp = new LinkedHashMap<>();
    private Iterator<Map.Entry<Field, Map.Entry<Integer, Integer>>> it;
    private TupleDesc td;
    private Tuple tp;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field af = tup.getField(afield), tf = gbfield == -1 ? null : tup.getField(gbfield);
        int new_val = ((IntField)af).getValue();
        int tmp = Integer.MIN_VALUE;
        Map.Entry<Integer, Integer> val;
        switch (what) {
            case MIN:
                tmp = Math.min(mp.getOrDefault(tf, new AbstractMap.SimpleEntry<>(-1, new_val)).getValue(), 
                      new_val);
                break;
            case MAX:
                tmp = Math.max(mp.getOrDefault(tf, new AbstractMap.SimpleEntry<>(-1, new_val)).getValue(), 
                      new_val);
                break;
            case COUNT:
            case SUM_COUNT:
            case SC_AVG:
            case AVG:
                cnt = mp.getOrDefault(tf, new AbstractMap.SimpleEntry<>(0, 0)).getKey() + 1;
            case SUM:
                tmp = mp.getOrDefault(tf, new AbstractMap.SimpleEntry<>(-1, 0)).getValue() + new_val;
                break;
        }
      
        if (what == Op.MIN || what == Op.MAX || what == Op.SUM) val = new AbstractMap.SimpleEntry<>(-1, tmp);
        else val = new AbstractMap.SimpleEntry<>(cnt, tmp);
        
        mp.put(tf, val);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new Operator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {
                super.open();
                it = mp.entrySet().iterator();
                if (what != Op.SUM_COUNT) 
                    td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "Val"});
                else
                    td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE, Type.INT_TYPE}, 
                                       new String[]{"groupVal", "sumVal", "countVal"});
            }
            
            @Override
            protected Tuple fetchNext() {
                if (!it.hasNext()) return null;
                Map.Entry<Field, Map.Entry<Integer, Integer>> ent = it.next();
                tp = new Tuple(td);
                
                tp.setField(0, ent.getKey());
                
                int sum = ent.getValue().getValue().intValue(), cnt = ent.getValue().getKey().intValue();
                if (what != Op.SUM_COUNT && sum != -1 && cnt != -1) sum /= cnt;
                
                tp.setField(1, new IntField(what == Op.COUNT ? cnt : sum));
                if (what == Op.SUM_COUNT) tp.setField(2, new IntField(cnt));
                
                
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
