package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId   tid;

    private OpIterator      child;
    private TupleDesc       td          = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean         isDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // DONE
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // DONE
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // DONE
        this.isDeleted = false;
        child.open();
        super.open();
    }

    public void close() {
        // DONE
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // DONE
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // DONE
        if (isDeleted) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, next);
                count++;
            } catch (TransactionAbortedException e) {
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error happen when delete tuple:" + e.getMessage());
            }
        }
        this.isDeleted = true;
        Tuple res = new Tuple(td);
        res.setField(0, new IntField(count));
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // DONE
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // DONE
        if (children.length == 0) {
            return;
        }
        this.child = children[0];
    }

}
