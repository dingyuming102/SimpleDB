package simpledb.util;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

// 仍可以使用消息队列来优化
public class HeapFileIterator implements DbFileIterator {
    private final TransactionId transactionId;
    private final int           tableId;
    private final int           pageNum;

    private int                 curPageId;
    private PageDataCachePool   pageDataCachePool;

    private boolean             isOpenFlag;

    public HeapFileIterator(final TransactionId transactionId, final HeapFile heapFile) {
        this(transactionId, heapFile.getId(), heapFile.numPages());
    }

    public HeapFileIterator(final TransactionId transactionId, final int tableId, final int pageNum) {
        this.transactionId  = transactionId;
        this.tableId        = tableId;
        this.pageNum        = pageNum;

        this.isOpenFlag     = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.pageDataCachePool  = new PageDataCachePool(this.pageNum, 0.2);
        this.curPageId          = 0;
        cacheFilePages();

        this.isOpenFlag         = true;
    }

    // Cache pages as many as have
    public void cacheFilePages() throws DbException, TransactionAbortedException {
        pageDataCachePool.close();


        for (; pageDataCachePool.hasFreeCache() && curPageId < pageNum; curPageId++) {
            final HeapPageId pageId = new HeapPageId(tableId, curPageId);
            final HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
            this.pageDataCachePool.offerPageData(page.iterator());
        }

        this.pageDataCachePool.open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!isOpenFlag) {
            return false;
        }

        // Cache another batch of pages
        while (!pageDataCachePool.hasNext()) {
            cacheFilePages();
            if (curPageId == pageNum) {
                break;
            }
        }
        return this.pageDataCachePool.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!isOpenFlag) {
            throw new NoSuchElementException("The iterator is NOT open.");
//            open();
        }

        if (!hasNext()) {
            throw new NoSuchElementException("The Iterator don't have more elements");
        }
        return this.pageDataCachePool.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        this.isOpenFlag = false;
        if (this.pageDataCachePool != null) {
            this.pageDataCachePool.close();
            this.pageDataCachePool = null;
        }
    }
}

