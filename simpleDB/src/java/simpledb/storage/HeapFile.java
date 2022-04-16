package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    private BufferPool bp;
    private int cur = 0;
    private RandomAccessFile rf;
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        try {
            rf = new RandomAccessFile(f, "rw");
        } catch (IOException e) {}
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgno = pid.getPageNumber();
        if (pgno < 0) throw new IllegalArgumentException("invalid PID");
        int sz = bp.getPageSize();
        byte[] res = new byte[sz];
        Page pg = null;
        try {
            rf.seek(pgno * sz);
            rf.read(res);
            pg = new HeapPage((HeapPageId)pid, res);
        } catch (Exception e) {}
        return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = ((HeapPage) page).getId();
        int pgno = pid.getPageNumber();
        try {
            rf.seek(pgno * bp.getPageSize());
            rf.write(page.getPageData());
        } catch (Exception e) {}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(f.length() / Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapPage pg;
        ArrayList<Page> inserted = new ArrayList<>();
        synchronized((Integer)getId()) {
            bp = Database.getBufferPool();
            int n = numPages();
            for (int i = 0; i < n; ++i) {
                pg = (HeapPage) bp.getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
                if (pg.getNumEmptySlots() > 0) {
                    pg.insertTuple(t);
                    pg.markDirty(true, tid);
                    inserted.add(pg);
                    return inserted;
                }
            }
            pg = new HeapPage(new HeapPageId(getId(), n), new byte[bp.getPageSize()]);
            pg.insertTuple(t);
            pg.markDirty(true, tid);
            writePage(pg);
        }
        return inserted;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage pg;
        ArrayList<Page> deleted = new ArrayList<>();
        synchronized((Integer)getId()) {
            bp = Database.getBufferPool();
            for (int i = 0, n = numPages(); i < n; ++i) {
                pg = (HeapPage) bp.getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
                try {
                    pg.deleteTuple(t);
                    pg.markDirty(true, tid);
                    deleted.add(pg);
                } catch (Exception e) {}
            }
        }
        return deleted;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                closed = false;
                pos = 0;
                iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pos++),        
                Permissions.READ_WRITE)).iterator();
            }
            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (iter != null && iter.hasNext()) return iter.next();
                int n = numPages();
                while (pos < n) {
                    iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pos++),        
                    Permissions.READ_WRITE)).iterator();
                    if (iter.hasNext()) return iter.next();
                }
                return null;
            }
            public void rewind() throws DbException, TransactionAbortedException {
                close(); open();
            }
            public void close() {
                super.close();
                pos = numPages();
            }
        };
    }

}

