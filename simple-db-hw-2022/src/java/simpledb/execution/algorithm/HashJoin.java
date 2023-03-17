package simpledb.execution.algorithm;

import simpledb.common.DbException;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;

// An impl of simple hashJoin
public class HashJoin extends JoinHelper {

    private static final long serialVersionUID = 1L;

    public HashJoin(JoinPredicate jp, OpIterator child1, OpIterator child2)
            throws TransactionAbortedException, DbException {
        super(jp, child1, child2);
    }

    @Override
    public Tuple fetchNext() {
        return null;
    }
}