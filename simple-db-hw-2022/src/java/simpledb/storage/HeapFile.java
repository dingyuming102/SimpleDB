package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.HeapFileIterator;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File       dbFile;
    private final TupleDesc  td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // DONE
        this.dbFile = f;
        this.td     = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // DONE
        return dbFile;
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
        // DONE
        return dbFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // DONE
        return td;
    }

    // see DbFile.java for javadocs
    // 此方法和iterator()方法最大的区别是，此方法绕过BufferPool，直接通过系统I/O读取外存文件数据。
    /* 读取 dbFile 里面的一页数据
     * 从 pageId 里面获取 读取第几页pageNo
     * 通过 BufferPool 获得每一页的大小 pageSize
     * 读取 pageNo * pageSize 之后的数据，
     * 因为我们是分页读取的，所以需要 skip 掉 pageNo * pageSize
     */
    public Page readPage(PageId pid) {
        // DONE
        if (!getFile().canRead()) {
            throw new IllegalArgumentException("HeapFile: readPage: The file can not be read.");
        }
        if (pid.getPageNumber() < 0 || pid.getPageNumber() >= this.numPages()) {
            throw new IllegalArgumentException("HeapFile: readPage: pageId out of range");
        }
        if (pid.getTableId() != getId()) {
            throw new IllegalArgumentException("HeapFile: readPage: pid.getTableId() != getId(): Page and file belongs to different table");
        }

        int                 pgNo        = pid.getPageNumber();
        int                 pageSize    = BufferPool.getPageSize();
        byte[]              rawPageData = HeapPage.createEmptyPageData();
        RandomAccessFile    raf         = null;
        HeapPage            heapPage    = null;
        try {
            raf = new RandomAccessFile(dbFile, "r");
            raf.seek((long) pgNo * pageSize);
            if (raf.read(rawPageData, 0, rawPageData.length) == -1) {
                return null;
            }
            heapPage = new HeapPage((HeapPageId) pid, rawPageData);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // DONE
        // not necessary for lab1
        if (!getFile().canWrite()) {
            throw new IOException();
        }

        HeapPageId          hPageId     = (HeapPageId) page.getId();
        if (hPageId.getTableId() != getId()) {
            throw new IllegalArgumentException("HeapFile: writePage: pid.getTableId() != getId(): Page and file belongs to different table");
        }
        int                 pgNo        = hPageId.getPageNumber();
        if (hPageId.getPageNumber() < 0 || hPageId.getPageNumber() > this.numPages()) {
            throw new IllegalArgumentException("HeapFile: writePage: pageId out of range");
        }
        int                 pageSize    = BufferPool.getPageSize();
        byte[]              rawPageData = page.getPageData();
        RandomAccessFile    raf         = null;
        try {
            raf = new RandomAccessFile(dbFile, "rws");
            raf.seek((long) pgNo * pageSize);
            raf.write(rawPageData);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // DONE
        return (int) dbFile.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE
        // not necessary for lab1
        ArrayList<Page> dirtyPageList = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
            HeapPage hPage = (HeapPage) Database.getBufferPool().
                    getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (hPage == null || hPage.getNumUnusedSlots() <= 0) {
                continue;
            }
            hPage.insertTuple(t);
            hPage.markDirty(true, tid);
            dirtyPageList.add(hPage);
            break;
        }
        // That means all pages are full, we should create a new page
        if (dirtyPageList.size() == 0) {
            HeapPageId  hPageId     = new HeapPageId(getId(), this.numPages());
            HeapPage    emptyPage   = new HeapPage(hPageId, HeapPage.createEmptyPageData());
            //-------注：HeapFile对象不应该操心写磁盘的事，什么时候落盘、该怎么写磁盘是BufferPool的事，由BufferPool统一调度。
            // 在这里HeapFile对象要维护的是：系统内的每一内存页都可以精准地映射到磁盘页上(页内数据不一致没关系)，不重不漏。
            // 所以在这里写一张空页到磁盘。
            writePage(emptyPage);
            // Through buffer pool to get newPage
            //-------注：HeapFile对象的writePage方法虽然负责具体如何写磁盘，但是HeapFile不应该操心什么时候写，什么时候落盘是BufferPool的事，由BufferPool统一调度。
            // 所以在这里，insertTuple只负责把数据写入内存，持久化的事交给BufferPool。
            // 如果一定要在这里持久化，那么顺序依然不能颠倒，先在操作系统内存中插入tuple然后写入磁盘，
            // 如果先写入磁盘再插入tuple则更新没写进去磁盘。
            HeapPage    hPage       = (HeapPage) Database.getBufferPool().getPage(tid, hPageId, Permissions.READ_WRITE);
            hPage.insertTuple(t);
            hPage.markDirty(true, tid);
            dirtyPageList.add(hPage);
        }
        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // DONE
        // not necessary for lab1
        final ArrayList<Page>   dirtyPageList = new ArrayList<>();
        final RecordId          recordId = t.getRecordId();
        final PageId            hPageId = recordId.getPageId();
        final HeapPage          hPage = (HeapPage) Database.getBufferPool().getPage(tid, hPageId, Permissions.READ_WRITE);
        if (hPageId.getTableId() != getId()) {
            throw new DbException("HeapFile: deleteTuple: hPageId.getTableId() != getId(): The tuple not a member of the file/table.");
        }
        if (hPage == null) {
            throw new DbException("HeapFile: deleteTuple: hPage == null: The page for this tuple NOT found.");
        }
        if (!hPage.isSlotUsed(recordId.getTupleNumber())) {
            throw new DbException("HeapFile: deleteTuple: !hPage.isSlotUsed(recordId.getTupleNumber()): " +
                    "The tuple cannot be deleted. The tuple has NOT reside in this page.");
        }
        hPage.deleteTuple(t);
        dirtyPageList.add(hPage);

        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // DONE
        return new HeapFileIterator(tid, this);

        /* 临时实现(为了过lab1的exercise 6)
         * hint中提到不要将所有tuple一次性放入内存
         */
//        return new DbFileIterator() {
//
//            private int             pgCursor    = -1;
//            private Iterator<Tuple> tupleIter   = null;
//            private Tuple           nextTuple   = null;
//
//            private boolean         isOpenFlag  = false;
//
//            @Override
//            public void open() throws DbException, TransactionAbortedException {
//                // Let iter be the first page's iterator.
//                pgCursor    = -1;
//                tupleIter   = nextTupleIter();
//                isOpenFlag  = true;
//            }
//
//            private boolean hasNextTupleIter() {
//                return pgCursor + 1 < numPages();
//            }
//
//            private Iterator<Tuple> nextTupleIter() throws TransactionAbortedException, DbException {
//                if (!hasNextTupleIter()) {
//                    return null;
//                }
//                PageId      pid     = new HeapPageId(getId(), ++pgCursor);
//                HeapPage    hPage   = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
//                return hPage.iterator();
//            }
//
//            private Tuple fetchNextTuple() throws DbException, TransactionAbortedException {
//                // 递归写法
////                if (tupleIter.hasNext()) {
////                    return tupleIter.next();
////                }
////                if (!hasNextTupleIter()) {
////                    return null;
////                }
////                tupleIter = nextTupleIter();
////                return fetchNextTuple();
//
//                // 迭代写法
//                while (true) {
//                    if (tupleIter.hasNext()) {
//                        return tupleIter.next();
//                    }
//                    if (!hasNextTupleIter()) {
//                        return null;
//                    }
//                    tupleIter = nextTupleIter();
//                }
//            }
//
//            @Override
//            public boolean hasNext() throws DbException, TransactionAbortedException {
//                if (!isOpenFlag) {
//                    return false;
//                }
//
//                if (nextTuple == null) {
//                    nextTuple = fetchNextTuple();
//                }
//                return nextTuple != null;
//            }
//
//            @Override
//            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//                if (!isOpenFlag) {
//                    throw new NoSuchElementException("The iterator is NOT open.");
//                }
//
//                if (nextTuple == null) {
//                    nextTuple = fetchNextTuple();
//                    if (nextTuple == null) {
//                        throw new NoSuchElementException();
//                    }
//                }
//
//                Tuple result = nextTuple;
//                nextTuple = null;
//                return result;
//            }
//
//            @Override
//            public void rewind() throws DbException, TransactionAbortedException {
//                close();
//                open();
//            }
//
//            @Override
//            public void close() {
//                pgCursor    = -1;
//                tupleIter   = null;
//                nextTuple   = null;
//                isOpenFlag  = false;
//            }
//        };
    }
}

