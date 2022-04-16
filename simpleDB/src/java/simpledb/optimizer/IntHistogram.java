package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min, max, sz, buckets, inc = Integer.MAX_VALUE, ntups = 0;
    private int[] heights;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
    	if (min != 0) {
    	    inc = -min;
    	    max += inc; 
    	    min = 0;
    	}
    	this.max = max; this.min = min;
    	sz = Math.max((int)Math.ceil((max + 1.0) / buckets), 1);
    	heights = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (inc != Integer.MAX_VALUE) v += inc;
        int bpos = v / sz, vpos = v % sz;
        if (bpos >= buckets || vpos >= sz) return;
        ++heights[bpos]; ++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (inc != Integer.MAX_VALUE) v += inc;
        int bpos = v / sz, vpos = v % sz;
        double res = 0;
        switch (op) {
            case EQUALS:
                if (v < min || v > max || heights[bpos] == 0 || ntups == 0) return 0;
                res = heights[bpos] / (sz * 1.0) / ntups;
                break;
            case GREATER_THAN: 
            case GREATER_THAN_OR_EQ: {
                if (v < min) return 1;
                if (v > max) return 0;
                res = heights[bpos] / (ntups * 1.0);
                if (op == Predicate.Op.GREATER_THAN) {
                    if (sz > 1) res = heights[bpos] / (ntups * 1.0) * ((bpos * sz + sz - v) / (sz * 1.0));
                } else {
                    res = heights[bpos] / (ntups * 1.0);
                    if (sz > 1) res *= (bpos * sz + sz - v + 1) / (sz * 1.0);
                }
                for (int i = bpos + 1; i < buckets; ++i) res += heights[i] / (ntups * 1.0);
                break;
            }
            case LESS_THAN: 
            case LESS_THAN_OR_EQ: {
                if (v < min) return 0;
                if (v > max) return 1;
                if (op == Predicate.Op.LESS_THAN) {
                    if (sz > 1) res = heights[bpos] / (ntups * 1.0) * ((v - bpos * sz) / (sz * 1.0));
                } else {
                    res = heights[bpos] / (ntups * 1.0);
                    if (sz > 1) res *= (v + 1 - bpos * sz) / (sz * 1.0);
                }
                for (int i = 0; i < bpos; ++i) res += heights[i] / (ntups * 1.0);
                break;
            } 
            case NOT_EQUALS: {
                if (v < min || v > max || heights[bpos] == 0 || ntups == 0) return 1;
                res = 1.0 - heights[bpos] / (sz * 1.0) / ntups;
                break;
            }
        }
        return res;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return null;
    }
}
