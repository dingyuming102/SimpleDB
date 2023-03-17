package simpledb.execution.algorithm;

import simpledb.common.DbException;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public class NestedLoopJoin extends JoinHelper {

    private static final long serialVersionUID = 1L;

    private Tuple left;
    private Tuple right;

    public NestedLoopJoin(JoinPredicate jp, OpIterator child1, OpIterator child2)
            throws DbException, TransactionAbortedException {
        super(jp, child1, child2);
        if (child1.hasNext()) {
            left = child1.next();
        }
        if (child2.hasNext()) {
            right = child2.next();
        }
    }

    @Override
    public Tuple fetchNext() throws DbException, TransactionAbortedException {
        while (left != null && right != null) {
            Tuple next = null;
            if (jp.filter(left, right)) {
                next = mergeTuple(left, right, td);
            }


            // fetch next items
            if (child2.hasNext()) {
                right = child2.next();
            } else {
                if (child1.hasNext()) {
                    left = child1.next();
                    child2.rewind();
                    if (child2.hasNext()) {
                        right = child2.next();
                    }
                } else {
                    left = null;
                    right = null;
                }
            }

            // return if exists
            if (next != null) {
                return next;
            }
        }
        return null;
    }
}
