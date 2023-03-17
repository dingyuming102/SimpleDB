package simpledb.execution.algorithm;

import simpledb.common.DbException;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public enum JoinStrategy {
    NestedLoop, SortMerge, Hash;

    private static final long serialVersionUID = 1L;
}
