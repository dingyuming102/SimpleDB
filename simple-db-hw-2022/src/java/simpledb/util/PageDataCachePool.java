package simpledb.util;

import simpledb.common.DbException;
import simpledb.storage.AbstractDbFileIterator;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

// Cache pool for file pages(tuples), cache for fileIterator
public class PageDataCachePool extends AbstractDbFileIterator {
    private final List<Iterator<Tuple>> tupleIterCache;
    private int                         cacheCapacity;
    private int                         curIndex;
    private Iterator<Tuple>             curIter;

//    private Tuple                       nextTuple;

    public PageDataCachePool(final int totalPage, final double cacheRate) {
        this((int) cacheRate * totalPage);
    }

    public PageDataCachePool(final int capacity) {
        this.cacheCapacity  = Math.max(1, capacity);
        this.tupleIterCache = new ArrayList<>();
    }

    public boolean offerPageData(final Iterator<Tuple> tupleIterator) {
        if (this.tupleIterCache.size() >= this.cacheCapacity) {
            return false;
        }

        this.tupleIterCache.add(tupleIterator);
        return true;
    }

//    public void init() {
//        this.curIndex = 0;
//        if (this.tupleIterCache.size() > 0) {
//            this.curIter = this.tupleIterCache.get(0);
//        }
//
//        this.nextTuple = null;
//    }

    @Override
    public void open() {
        this.curIndex = 0;
        if (this.tupleIterCache.size() > 0) {
            this.curIter = this.tupleIterCache.get(0);
        }

    }

//    public void clear() {
//        this.tupleIterCache.clear();
//        this.curIter = null;
//    }

    @Override
    public void close() {
        super.close();
        this.tupleIterCache.clear();
    }

    @Override
    public void rewind() {
        close();
        open();
    }

    // Return next tuple if exists,
    // otherwise return null.
//    private Tuple fetchNextTuple() {
//        if (this.curIter == null || this.tupleIterCache.size() == 0) {
//            return null;
//        }
//
//
//        while (!this.curIter.hasNext() && this.curIndex + 1 < this.tupleIterCache.size()) {
//            this.curIndex++;
//            this.curIter = this.tupleIterCache.get(this.curIndex);
//        }
//        if (this.curIter.hasNext()) {
//            return this.curIter.next();
//        } else {
//            return null;
//        }
//    }

    // Return next tuple if exists,
    // otherwise return null.
    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (this.curIter == null || this.tupleIterCache.size() == 0) {
            return null;
        }


        while (!this.curIter.hasNext() && this.curIndex + 1 < this.tupleIterCache.size()) {
            this.curIndex++;
            this.curIter = this.tupleIterCache.get(this.curIndex);
        }
        if (this.curIter.hasNext()) {
            return this.curIter.next();
        } else {
            return null;
        }
    }

    // Return true if next tuple exists,
    // otherwise false.
//    public boolean hasNextTuple() {
//        if (nextTuple == null) {
//            nextTuple = fetchNextTuple();
//        }
//        return nextTuple != null;
//    }

    // Return next tuple if exists,
    // otherwise throw an exception.
//    public Tuple nextTuple() {
//        if (nextTuple == null) {
//            nextTuple = fetchNextTuple();
//            if (nextTuple == null) {
//                throw new NoSuchElementException();
//            }
//        }
//
//        Tuple result = nextTuple;
//        nextTuple = null;
//        return result;
//    }

    public boolean hasFreeCache() {
        return tupleIterCache.size() < cacheCapacity;
    }

    public int getCacheCapacity() {
        return cacheCapacity;
    }
}

