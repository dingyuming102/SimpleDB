package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.LRUCache;

import java.io.IOException;
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
// BufferPool类似于银行，DbFile相当于储户。
// 储户(DbFile)拥有赚钱(通过系统I/O读取文件数据)的能力。
// 但是储户不直接碰、管理钱(数据)，而是委托银行(BufferPool)收、存、管理钱。
// 储户需要用钱时，用身份证(TransactionId)和电子存单(PageId)从银行取钱。
// 为防止同一份钱(数据)被不同的人(事务)用相同电子存单同时取出，所以需要身份证(TransactionId)来识别权限。
// 使用BufferPool的好处是，使用一个individual实体来统一管理数据的读写，以保证数据库的ACID properties。
// 我们不必在各个DbFile实现类中实现线程安全，线程安全问题全部托管给BufferPool来实现。
// 各个DbFile实现类之间也感知不到互相的存在。符合低耦合高内聚的设计理念。
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int        DEFAULT_PAGE_SIZE   = 4096;

    private static int              pageSize            = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int         DEFAULT_PAGES       = 50;

    private final int                       maxPageNum;
    private volatile LRUCache<PageId, Page> lruCache;       // LRUCache有线程安全的内部实现，在当前业务场景也不可能出现线程安全问题(同时写同一页)
    private volatile LockManager            lockManager;    // LockManager有线程安全的内部实现，对其操作不需上锁

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // NOT YET COMPLETELY DONE
        this.maxPageNum     = numPages;
        this.lruCache       = new LRUCache<>(numPages);
        this.lockManager    = new LockManager();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // COMPLETELY DONE

        // acquire the lock
        try {
            lockManager.acquireLock(tid, pid, perm);
        } catch (TransactionAbortedException e) {
            throw e;
        }


        if (lruCache.containsKey(pid)) {
            return lruCache.get(pid);
        }

        Page page = Database.getCatalog()
                            .getDatabaseFile(pid.getTableId())
                            .readPage(pid);
        addOrUpdatePage(pid, page);

        return page;
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
        // DONE
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // DONE
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // DONE
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // DONE
        // not necessary for lab1|lab2
        Set<PageId> lockedPageSet = lockManager.getLockedPage(tid);
        if (lockedPageSet == null) {   // this transaction may acquire no lock
            return;
        }
        try {
            for (PageId pid : lockedPageSet) {
                synchronized (pid) {
                    if (!lruCache.containsKey(pid)) {
                        continue;
                    }
                    Page page = lruCache.get(pid);
                    if (commit) {
                        flushPage(pid, page);

                        // use current page contents as the before-image
                        // for the next transaction that modifies this page.
                        page.setBeforeImage();
                    } else {
                        addOrUpdatePage(page.getId(), page.getBeforeImage());
                    }
                }
            }
        } catch (DbException | IOException e) {
            e.printStackTrace();
        } finally {
            // 尽量不在遍历时release，因为会修改底层数据结构，
            // 尽管底层用的是线程安全的ConcurrentHashMap，
            // 但，君子不立于危墙之下。
            lockManager.releaseLock(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE
        // not necessary for lab1
        DbFile      table       = Database.getCatalog().getDatabaseFile(tableId);
        List<Page>  dirtyPages  = table.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            addOrUpdatePage(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE
        // not necessary for lab1
        int         tableId     = t.getRecordId().getPageId().getTableId();
        DbFile      table       = Database.getCatalog().getDatabaseFile(tableId);
        List<Page>  dirtyPages  = table.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            addOrUpdatePage(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        // DONE
        // not necessary for lab1
        for (Map.Entry<PageId, Page> e: this.lruCache.entrySet()) {
            PageId pid = e.getKey();
            synchronized (pid) {
                flushPage(pid, e.getValue());
            }
        }
        return;
    }

    public void addOrUpdatePage(PageId pid, Page page) throws DbException {
        if (pid == null || page == null) {
            System.out.println("CANNOT add page to LRUCache. Because the page is null!");
        }
        synchronized (pid) {
            while (lruCache.size() > maxPageNum || (lruCache.size() == maxPageNum && !lruCache.containsKey(pid))) {
                evictPage();
            }
            lruCache.put(pid, page);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public void removePage(PageId pid) {
        // DONE
        // not necessary for lab1
        synchronized (pid) {
            if (!lruCache.containsKey(pid)) {
                // To do: this happens a lot. Figure out why
                System.out.println("removing non-existing page");
                return;
            }
            this.lruCache.remove(pid);
        }
    }

    public boolean discardPage(PageId pid) {
        // DONE
        // not necessary for lab1
        synchronized (pid) {
            if (!lruCache.containsKey(pid)) {
                // To do: this happens a lot. Figure out why
                System.out.println("discarding non-existing page");
                return false;
            }
            this.lruCache.remove(pid);
            return true;
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        // DONE
        // not necessary for lab1
        synchronized (pid) {
            flushPage(pid, lruCache.get(pid));
        }
    }

    private void flushPage(PageId pid, Page page) throws IOException {
        // NOE YET COMPLETELY DONE
        // not necessary for lab1
        synchronized (pid) {
            try {
                // for lab6, write update record first
                final LogFile logFile = Database.getLogFile();
                if (page.isDirty() != null) {
                    logFile.logWrite(page.isDirty(), page.getBeforeImage(), page);
                    logFile.force();
                }

                // Write page
                DbFile tableFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                tableFile.writePage(page);
                page.markDirty(false, null);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Error happen when flush page to disk:" + e.getMessage());
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        // DONE
        // not necessary for lab1|lab2
        Set<PageId> pageIds = lockManager.getLockedPage(tid);
        if (pageIds == null) {  // tid may acquire no lock
            return;
        }
        for (PageId pid : pageIds) {
            synchronized (pid) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        // DONE
        // not necessary for lab1

        // LRUCache底层用的数据结构是ConcurrentHashMap，
        // 所以可以线程安全地在遍历时修改
        for (Map.Entry<PageId, Page> e: this.lruCache.entrySet()) {
            PageId pid = e.getKey();
            synchronized (pid) {
                if (e.getValue().isDirty() != null) {
                    continue;
                }
                if (!discardPage(e.getKey())) {
                    continue;
                }
                return;
            }
        }
        throw new DbException("All pages are dirty in buffer pool");
    }

}
