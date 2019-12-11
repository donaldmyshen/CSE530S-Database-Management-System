 
package hw4;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hw1.Database;
import hw1.HeapPage;
import hw1.Tuple;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */
public class BufferPool {

    private int numPages; // number of heap pages in the buffer pool
    private HashMap<String, HeapPage> cache = new HashMap<>();
    private HashMap<Integer, List<String>> pagesMap = new HashMap<>();
    private HashMap<String, List<Integer>> readLocks = new HashMap<>();  // read lock for heap page
    private HashMap<String, Integer> writeLocks = new HashMap<>(); // write lock for heap page

    /**
     * Bytes per page, including header.
     */
    // seems no use?
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, an page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid     the ID of the transaction requesting the page
     * @param tableId the ID of the table with the requested page
     * @param pid     the ID of the requested page
     * @param perm    the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm) throws Exception {
        // Check whether transaction can get a lock on that page or not, and acquire a lock or wait to abort
        String key = tableId + ":" + pid;
        if (writeLocks.containsKey(key) && writeLocks.get(key) != tid) {
            // abortion
            transactionComplete(tid, false);
            return null;
        }
        if (!cache.containsKey(key) && cache.size() == numPages)
            //throw exception
            evictPage();
        //read
        if (perm == Permissions.READ_ONLY) {
            List<Integer> tidList = readLocks.getOrDefault(key, new ArrayList<>());
            if (!tidList.contains(tid))
                tidList.add(tid);
            readLocks.put(key, tidList);
        } else {
            // update read to write
            writeLocks.put(key, tid);
        }
        List<String> pageList = pagesMap.getOrDefault(tid, new ArrayList<>());
        if (!pageList.contains(key))
            pageList.add(key);
        pagesMap.put(tid, pageList);
        HeapPage page = Database.getCatalog().getDbFile(tableId).readPage(pid);
        cache.put(key, page);
        return page;
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid     the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid     the ID of the page to unlock
     */
    public void releasePage(int tid, int tableId, int pid) {
        String key = tableId + ":" + pid;
        // Remove the write lock
        if (writeLocks.containsKey(key) && writeLocks.get(key) == tid) {
            writeLocks.remove(key);
        }
        // Remove the read lock
        if (readLocks.containsKey(key)) {
            List<Integer> tidList = readLocks.get(key);
            tidList.remove(tid);
            if (tidList.size() == 0) {
                readLocks.remove(key);
            }
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(int tid, int tableId, int pid) {
        String key = tableId + ":" + pid;
        // Check if tid holds read lock
        if (readLocks.containsKey(key))
            return readLocks.get(key).contains(tid);
        // Check if tid holds write lock
        return writeLocks.containsKey(key) && writeLocks.get(key) == tid;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction. If the transaction wishes to commit, write
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(int tid, boolean commit) throws IOException {
        if (!pagesMap.containsKey(tid)) {
            throw new IOException("No such transaction");
        }
        for (String data : pagesMap.get(tid)) {
            int tableId = Integer.parseInt(data.substring(0, 10));
            int pId = Character.getNumericValue(data.charAt(data.length() - 1));
            // Release the lock
            releasePage(tid, tableId, pId);
            if (cache.get(data).isDirty() && cache.get(data).getTransactionId() == tid) {
                if (commit)
                    flushPage(tableId, pId);
                else
                    cache.remove(data);
            }
        }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid. Will acquire a
     * write lock on the page the tuple is added to. May block if the lock cannot be
     * acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(int tid, int tableId, Tuple tuple) throws Exception {
        String key = tableId + ":" + tuple.getPid();
        //if the transaction holds the write lock
        if (writeLocks.containsKey(key) && writeLocks.get(key) == tid) {
            HeapPage page = cache.get(key);
            page.setDirty(tid, true);
            page.addTuple(tuple);
            cache.put(key, page);
        } else {
            //read only, throw exception
            throw new IOException("Read only file! No permision to write!");
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from. May block if the lock cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid     the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t       the tuple to add
     */
    public void deleteTuple(int tid, int tableId, Tuple tuple) throws Exception {
        String key = tableId + ":" + tuple.getPid();
        // if the transaction holds the lock
        if (writeLocks.containsKey(key) && writeLocks.get(key) == tid) {
            HeapPage page = cache.get(key);
            page.setDirty(tid, true);
            page.deleteTuple(tuple);
            cache.put(key, page);
        } else {
            //read only, throw exception
            throw new IOException("Read only file! No permision to write!");
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void flushPage(int tableId, int pid) throws IOException {
        String key = tableId + ":" + pid;
        // if the buffer pool contains the key
        if (cache.containsKey(key) && cache.get(key).isDirty()) {
            Database.getCatalog().getDbFile(tableId).writePage(cache.get(key));
            cache.get(key).setDirty(-1, false); // set as clean and clear the transaction id
        } else {
            throw new IOException("No such page in the cache!");
        }
    }

    /*
     * * Since space in the BufferPool cache is limited, you may have to sometimes
     * evict pages when reading in a new page. We want to ensure that we do not
     * evict any dirty pages, as these pages are currently being worked on by a
     * transaction. Complete your evict() method such that it evicts the first
     * non-dirty page that it can find. If no such page exists, throw an exception.
     *
     */
    private synchronized void evictPage() throws Exception {
        for (Map.Entry<String, HeapPage> e : cache.entrySet()) {
            if (!e.getValue().isDirty()) {
                cache.remove(e.getKey());
                // Remove keys
                for (List<String> list : pagesMap.values())
                    list.remove(e.getKey());
                // Remove locks
                readLocks.remove(e.getKey());
                writeLocks.remove(e.getKey());
                return;
            }
        }
        throw new Exception("All pages are dirty! Can not evict page!");
    }
}