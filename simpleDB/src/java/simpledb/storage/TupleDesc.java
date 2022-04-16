package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> arr = new ArrayList<>();
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return arr.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int len = typeAr.length;
        if (len == 0) return;
        for (int i = 0; i < len; ++i) arr.add(new TDItem(typeAr[i], fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int len = typeAr.length;
        if (len == 0) return;
        for (int i = 0; i < len; ++i) arr.add(new TDItem(typeAr[i], null));
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return arr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= arr.size() || i < 0) throw new NoSuchElementException("index out of range");
        return arr.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= arr.size() || i < 0) throw new NoSuchElementException("index out of range");
        return arr.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) throw new NoSuchElementException("name must not be null");
        int i = 0, l = arr.size();
        for (; i < l; ++i) if (arr.get(i).fieldName != null && arr.get(i).fieldName.equals(name)) break;
        if (i == l) throw new NoSuchElementException("name unfound");
        return i;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int res = 0;
        for (int i = 0, j = arr.size(); i < j; ++i) res += arr.get(i).fieldType.getLen();
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int m = td1.numFields(), n = td2.numFields();
        Type[] t = new Type[m + n];
        String[] f = new String[m + n];
        Iterator<TDItem> it1 = td1.iterator(), it2 = td2.iterator();
        TDItem ti;
        for (int i = 0; i < m + n; ++i) {
             ti = i < m ? it1.next() : it2.next();
             t[i] = ti.fieldType; f[i] = ti.fieldName;
        }
        
        return new TupleDesc(t, f);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) return false;
        int m = ((TupleDesc)o).numFields();
        if (numFields() != m) return false;
        Iterator<TDItem> it1 = iterator(), it2 = ((TupleDesc)o).iterator();
        while (it1.hasNext()) if (it1.next().fieldType != it2.next().fieldType) return false;
        return true;
    }

    public int hashCode() {
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    
    public String toString() {
        String res = "";
        for (int i = 0, l = arr.size(); i < l; ++i) 
            res += arr.get(i).fieldType + "(" + arr.get(i).fieldName + ")" + (i == l - 1 ? "" : ", ");
        return res;
    }
}
