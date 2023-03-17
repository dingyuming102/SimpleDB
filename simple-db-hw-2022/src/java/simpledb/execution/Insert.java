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
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId     tid;

    private OpIterator        child;
    private int               tableId;
    private TupleDesc         td        = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean           isFetched;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // DONE
        this.tid        = t;
        this.child      = child;
        this.tableId    = tableId;
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("Insert: Insert: The TupleDesc of given child do not match that of given tableId.");
        }
    }

    public TupleDesc getTupleDesc() {
        // DONE
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // DONE
        this.isFetched = false;
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // DONE
        if (isFetched) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, next);
                count++;
            } catch (TransactionAbortedException e) {
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error happen when insert tuple:" + e.getMessage());
            }
        }
        this.isFetched = true;
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
