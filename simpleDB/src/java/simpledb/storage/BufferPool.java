package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.concurrent.*;
import java.util.*;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    public class Locksmith {
        private ConcurrentHashMap<PageId, ArrayList<TransactionId>> reader_id = new ConcurrentHashMap<>();
        private ConcurrentHashMap<PageId, TransactionId> writer_id = new ConcurrentHashMap<>();
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> wait_for = new ConcurrentHashMap<>();
        private Semaphore lock;
        private ConcurrentHashMap<PageId, Semaphore> locks= new ConcurrentHashMap<>();
        private final long TIME_BOUND;
           
        public Locksmith() {
            this(1000);
        }
        
        public Locksmith(long tBound) {
            TIME_BOUND = tBound;
        }
        
        public boolean holdsLock(TransactionId tid, PageId pid) {
            return (reader_id.containsKey(pid) && reader_id.get(pid).contains(tid)) || 
                   (writer_id.containsKey(pid) && writer_id.get(pid) == tid);
        }
        
        private boolean acquire_lock(PageId pid, TransactionId tid) throws TransactionAbortedException {
            lock = locks.getOrDefault(pid, new Semaphore(1));
            boolean locked = false;
            tryBreakCycle(tid);
            locked = lock.tryAcquire();
            if (!locks.containsKey(pid)) locks.put(pid, lock);
            if (locked && wait_for.containsKey(tid)) wait_for.remove(tid);
            return locked;
        }
        
        private void done_waiting(TransactionId tid) {
            Iterator<Map.Entry<TransactionId, HashSet<TransactionId>>> it = wait_for.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TransactionId, HashSet<TransactionId>> entry = it.next();
                if (entry.getValue().contains(tid)) entry.getValue().remove(tid);
                if (entry.getValue().isEmpty()) it.remove();
            }
        }
        
        private void release_lock(PageId pid) {
            try { locks.get(pid).release(); } catch (Exception e) {}
        }
        
        public void acquireLock(TransactionId tid, PageId pid, Permissions perm) 
        throws TransactionAbortedException {
            while (true) {
                synchronized((Integer)pid.hashCode()) {
                    if (writer_id.containsKey(pid) && writer_id.get(pid) == tid) return;
                    switch (perm) {
                        case READ_ONLY: {
                            if (!reader_id.containsKey(pid)) {  
                                if (acquire_lock(pid, tid)) {
                                    reader_id.put(pid, new ArrayList<>(Arrays.asList(tid)));
                                    return;
                                }
                            } else {
                                wait_for.remove(tid);
                                reader_id.get(pid).add(tid);
                                return;
                            }
                            wait_for.computeIfAbsent(tid, k -> new HashSet<>()).add(writer_id.get(pid));
                            relinquish(tid);
                            break; 
                        }
                        case READ_WRITE: {
                            if (reader_id.containsKey(pid) && reader_id.get(pid).get(0) == tid) {
                                writer_id.put(pid, tid);
                                reader_id.remove(pid);
                                return;
                            } else if (acquire_lock(pid, tid)) {
                                writer_id.put(pid, tid);
                                return;
                            }
                            if (writer_id.containsKey(pid))
                                wait_for.computeIfAbsent(tid, k -> new HashSet<>()).add(writer_id.get(pid));
                            else if (reader_id.containsKey(pid))
                                wait_for.computeIfAbsent(tid, k -> new HashSet<>()).add(reader_id.get(pid).get(0));
                            relinquish(tid);
                            break;
                        }
                        default: throw new TransactionAbortedException();
                    }
                }
            }
        }
        
        private boolean findCycle(TransactionId tid, HashSet<TransactionId> vis, HashSet<TransactionId> stk) {
            if (!vis.contains(tid)) {
                vis.add(tid); stk.add(tid);
                if (wait_for.containsKey(tid)) 
                    for (TransactionId t : wait_for.get(tid))
                        if (stk.contains(t) || (!vis.contains(t) && findCycle(t, vis, stk))) 
                            return true;
            }              
            stk.remove(tid);
            return false;
        }
        
        private void tryBreakCycle(TransactionId tid) throws TransactionAbortedException {
            if (!wait_for.containsKey(tid)) return;
            if (findCycle(tid, new HashSet<>(), new HashSet<>())) throw new TransactionAbortedException();
        }
     
        public void releaseLock(TransactionId tid, PageId pid) {
            if (tid == null) {
                if (reader_id.containsKey(pid)) {
                    for (TransactionId t : reader_id.get(pid)) done_waiting(t);
                    reader_id.remove(pid);
                }
                if (writer_id.containsKey(pid)) {
                    done_waiting(writer_id.get(pid));
                    writer_id.remove(pid);
                }
                if (locks.containsKey(pid)) {
                    release_lock(pid);
                    locks.remove(pid); 
                }
                return;
            }
            done_waiting(tid);
            if (pid == null) {
                Iterator<Map.Entry<PageId, TransactionId>> witer = writer_id.entrySet().iterator();
                while (witer.hasNext()) {
                    Map.Entry<PageId, TransactionId> entry = witer.next();
                    if(entry.getValue() == tid) {
                        release_lock(entry.getKey());
                        witer.remove();
                    }
                }
                    
                Iterator<Map.Entry<PageId, ArrayList<TransactionId>>> riter = reader_id.entrySet().iterator();
                while (riter.hasNext()) {
                    Map.Entry<PageId, ArrayList<TransactionId>> entry = riter.next();
                    if(entry.getValue().contains(tid)){
                        entry.getValue().remove(tid);
                        if (entry.getValue().size() == 0) {
                            release_lock(entry.getKey());
                            riter.remove();
                        }
                    }
                }
            }
            else if (writer_id.containsKey(pid) && writer_id.get(pid) == tid)  {
                writer_id.remove(pid);
                release_lock(pid);
            } 
            else if (reader_id.containsKey(pid) && reader_id.get(pid).contains(tid)) {
                reader_id.get(pid).remove(tid);
                if (reader_id.get(pid).size() == 0) {
                    release_lock(pid);
                    reader_id.remove(pid);
                }
            }
        }
        
        private void relinquish(TransactionId tid) throws TransactionAbortedException {
            long time_used = System.currentTimeMillis() - tid.getStartTime();
            if (time_used >= TIME_BOUND) throw new TransactionAbortedException();
            Thread.yield();
        }
    }
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
    protected ConcurrentHashMap<PageId, Page> pge = new ConcurrentHashMap<>();
    private Locksmith lock;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        lock = new Locksmith();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }
    
    public Locksmith getLock() {
        return lock;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        Page p = null;
        synchronized((Long)tid.getId()) {
            p = pge.get(pid);
            if (p == null) {
                p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                if (pge.size() == numPages) evictPage();
                pge.put(pid, p);
            }        
            lock.acquireLock(tid, pid, perm);
        }
        return p;
    }
    
    private void updateCache(List<Page> pages) throws DbException {
        for (Page p : pages) {
            if (pge.size() == numPages) evictPage();
            pge.put(p.getId(), p);
        }
    }
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            transactionComplete(tid, true);
        } catch (Exception e) {}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return lock.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        try {
            if (commit) flushPages(tid, commit);
            else rollbackPages(tid);
        } catch (Exception e) {}
        lock.releaseLock(tid, null);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */  
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        updateCache(Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t));
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        updateCache(Database.getCatalog().getDatabaseFile(
        t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t));
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p : pge.values()) {
            if (p.isDirty() == null) continue;
            flushPage(p.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pge.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (pid == null) return;
        Page p = pge.get(pid);
        if (p.isDirty() != null){
          Database.getLogFile().logWrite(p.isDirty(), p.getBeforeImage(), p);
          Database.getLogFile().force();
          Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
          p.markDirty(false, null);
        }  
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid, boolean commit) throws IOException {
        for (Page p : pge.values()) {
            if (p.isDirty() != tid) continue;
            flushPage(p.getId());
            if (commit) p.setBeforeImage();
        }
    }
    
    private synchronized void rollbackPages(TransactionId tid) throws IOException {
        for (Page p : pge.values()) {
            if (p.isDirty() != tid) continue;
            discardPage(p.getId());
        }
    }

    /**
     * Discards a clean page from the buffer pool.
     */
    private synchronized void evictPage() throws DbException {
        for (Page pg : pge.values()) {
            if (pg.isDirty() != null) continue;
            discardPage(pg.getId());
            return;
        }
        throw new DbException("all pages are dirty");    
    }

}
