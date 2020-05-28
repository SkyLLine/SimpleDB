package simpledb;

import java.io.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private ConcurrentHashMap<PageId, Page>map;

    private int numPages;

    private LockManager lockManager;

    private ConcurrentHashMap<PageId, Page> usedmap;


    private static int pageSize = DEFAULT_PAGE_SIZE;

    private ConcurrentHashMap<TransactionId, Long>Transactions;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        map = new ConcurrentHashMap<>();
        usedmap = new ConcurrentHashMap<>();
        Transactions = new ConcurrentHashMap<>();
        lockManager = new LockManager();
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
        // some code goes here
        if(!Transactions.containsKey(tid)){
            Transactions.put(tid, System.currentTimeMillis());
        }
        long timestart = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000)+1000;

        boolean result = lockManager.grantLock(tid, pid, perm);
        while(!result){
            long now = System.currentTimeMillis();
            if(now - timestart >timeout){
                throw new TransactionAbortedException();
            }
            result = lockManager.grantLock(tid, pid, perm);
        }

        if(map.containsKey(pid)){
            return map.get(pid);
        }else{
//            System.out.print(pid.getTableId()+" ");
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if(numPages == map.size()){
                evictPage();
            }
            map.put(pid,page);
            return page;
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
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        ArrayList<Lock> list = lockManager.lockPageMap.get(pid);
        Lock lock = lockManager.getLock(tid, pid);
        if(lock != null){
            list.remove(lock);
            lockManager.lockPageMap.put(pid, list);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit == true){
            flushPages(tid);
        }else{
            for (Page page : map.values()) {
                if (page.isDirty()!=null && page.isDirty().equals(tid)) {
                    map.put(page.getId(), page.getBeforeImage());
                }
            }
        }
        lockManager.releaseTidlock(tid);
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
            throws DbException, IOException, TransactionAbortedException{
        // some code goes here
        // not necessary for lab1
        List<Page> a = new ArrayList<>();
        DbFile file =  Database.getCatalog().getDatabaseFile(tableId);
//        System.out.print(3);
        a = file.insertTuple(tid, t);
        for(int i = 0; i < a.size(); i++){
            a.get(i).markDirty(true, tid);
            map.put(a.get(i).getId(),a.get(i));
        }
//        System.out.print(4);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException{
        // some code goes here
        // not necessary for lab1
        List<Page> a;
        int tableid = t.getRecordId().getPageId().getTableId();
        DbFile file = (DbFile) Database.getCatalog().getDatabaseFile(tableid);
        a = file.deleteTuple(tid, t);
        for(int i = 0; i < a.size(); i++){
            if(a.get(i).isDirty() != null){
                map.put(a.get(i).getId(),a.get(i));
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for( PageId pageId : map.keySet()){
            flushPage(pageId);
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
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = map.get(pid);
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(page);
        page.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(PageId pageid: map.keySet()){
            if(map.get(pageid).isDirty() != null && map.get(pageid).isDirty() == tid){
                flushPage(map.get(pageid).getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int num = 0;
        for(PageId pid : map.keySet()){
            if(((HeapPage)map.get(pid)).dirty()){
                num++;
            }
        }
        if(num == numPages){
            throw new DbException("");
        }
        for(PageId pid : map.keySet()){
            Page page = map.get(pid);
            if(!((HeapPage)page).dirty()){
                map.remove(page);
                break;
            }
        }
    }

    private class LockManager{
        private ConcurrentHashMap<PageId, ArrayList<Lock>> lockPageMap;

        public LockManager(){
            lockPageMap = new ConcurrentHashMap<>();
        }

        public Lock getLock(TransactionId tid, PageId pid)
        {
            ArrayList<Lock> locklist = lockPageMap.get(pid);
            if(locklist == null)
            {
                return null;
            }else {
                for(Lock lock:locklist)
                {
                    if(lock.getId().equals(tid)){
                        return lock;
                    }
                }
            }
            return null;
        }

        public synchronized void releaseLock(TransactionId tid)
        {
            List<PageId> pid = new ArrayList<>();
            for(Map.Entry<PageId, ArrayList<Lock>> entry:lockPageMap.entrySet()){
                for(Lock lock: entry.getValue()){
                    if(lock.getId().equals(tid)){
                        pid.add(entry.getKey());
                    }
                }
            }
            for(PageId p:pid){
                ArrayList<Lock> lock = new ArrayList<>();
                lock = lockPageMap.get(pid);
                if(lock != null && lock.size() != 0)
                {
                    Lock l = getLock(tid, p);
                    if(l != null){
                        lock.remove(l);
                        lockPageMap.put(p,lock);
                    }
                }
            }
        }

        public synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm){
            Lock l;
            if(perm == Permissions.READ_WRITE)
            {
                l = new Lock(tid, false, perm);
            }else{
                l = new Lock(tid, true, perm);
            }
            ArrayList<Lock> list = lockPageMap.get(pid);
            if(list == null)
            {
                list = new ArrayList<>();
            }
            list.add(l);
            lockPageMap.put(pid, list);
            return true;
        }

        public boolean holdsLock(TransactionId tid, PageId pid)
        {
            if(getLock(tid, pid)!=null){
                return true;
            }
            return false;

        }

        public synchronized void releaseTidlock(TransactionId tid){
            ArrayList<PageId> p = new ArrayList<>();
            for(Map.Entry<PageId, ArrayList<Lock>> entry : lockPageMap.entrySet()){
                for(Lock lock : entry.getValue()){
                    if(lock.getId().equals(tid)){
                        p.add(entry.getKey());
                    }
                }
            }
            for( PageId pid : p){
                Lock lock = getLock(tid, pid);
                if(lock != null){
                    ArrayList<Lock> locklist = lockPageMap.get(pid);
                    locklist.remove(lock);
                    lockPageMap.put(pid, locklist);
                }
            }
        }

        public boolean grantLock(TransactionId tid, PageId pid, Permissions perm){
            ArrayList<Lock> list =lockPageMap.get(pid);
            if(perm == Permissions.READ_ONLY){
                if(list != null && list.size() != 0){
                    if(list.size() == 1){
                        Lock lock =list.iterator().next();
                        if(lock.getId().equals(tid)){
                              return lock.getPerm() == Permissions.READ_ONLY || lock(pid,tid, perm);
                            }else{
                                return lock.getPerm() == Permissions.READ_ONLY && lock(pid, tid, Permissions.READ_ONLY);
                            }
                    }else{
                        for(Lock l : list){
                            if(l.getPerm() == Permissions.READ_WRITE){
                                return l.getId().equals(tid);
                            }else if(l.getId().equals(tid)){
                                return true;
                            }
                        }
                        return lock(pid, tid, Permissions.READ_ONLY);
                    }
                }else{
                    return lock(pid, tid, Permissions.READ_ONLY);
                }
            }else{
                if(list != null && list.size() != 0){
                    if(list.size() == 1) {
                        Lock lock = list.iterator().next();
                        return lock.getId().equals(tid) && (lock.getPerm() == Permissions.READ_WRITE || lock(pid, tid, Permissions.READ_WRITE));
                        }else{
                        if(list.size() == 2){
                            for(Lock l : list){
                                if(l.getId().equals(tid) && l.getPerm() == Permissions.READ_WRITE){
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                }else{
                    return lock(pid, tid, Permissions.READ_WRITE);
                }
            }
        }

    }
}

