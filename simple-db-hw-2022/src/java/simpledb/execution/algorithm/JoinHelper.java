package simpledb.execution.algorithm;

import simpledb.common.DbException;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public abstract class JoinHelper {

    private static final long serialVersionUID = 1L;

    protected final JoinPredicate   jp;
    protected final OpIterator      child1;
    protected final OpIterator      child2;
    protected final TupleDesc       td;

    public JoinHelper(JoinPredicate jp, OpIterator child1, OpIterator child2)
            throws DbException, TransactionAbortedException {
        this.jp         = jp;
        this.child1     = child1;
        this.child2     = child2;
        this.td         = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }


    public abstract Tuple fetchNext() throws DbException, TransactionAbortedException;

    public static Tuple mergeTuple(final Tuple tuple1, final Tuple tuple2, final TupleDesc td) {
        final Tuple tuple   = new Tuple(td);
        final int   len1    = tuple1.getTupleDesc().numFields();
        for (int i = 0; i < td.numFields(); i++) {
            if (i < len1) {
                tuple.setField(i, tuple1.getField(i));
            } else {
                tuple.setField(i, tuple2.getField(i - len1));
            }
        }

        return tuple;
    }
}
