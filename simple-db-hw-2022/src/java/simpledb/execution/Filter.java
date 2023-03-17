package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate         p;
    private OpIterator        child;
    private TupleDesc         td;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // DONE
        this.p      = p;
        this.child  = child;
        this.td     = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // DONE
        return p;
    }

    public TupleDesc getTupleDesc() {
        // DONE
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // DONE
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
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // DONE
        Tuple tp;
        while (child.hasNext()) {
            tp = child.next();
            if (p.filter(tp)) {
                return tp;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // DONE
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // DONE
        this.child  = children[0];
        this.td     = child.getTupleDesc();
    }

}
