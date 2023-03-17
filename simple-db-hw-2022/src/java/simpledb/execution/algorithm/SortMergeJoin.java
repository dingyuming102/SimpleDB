package simpledb.execution.algorithm;

import simpledb.common.DbException;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SortMergeJoin extends JoinHelper {

    private static final long serialVersionUID = 1L;

    private final int     blockCacheSize = 131072 * 5;
    private Tuple[]       block1;
    private Tuple[]       block2;

    private JoinPredicate lt;
    private JoinPredicate eq;

    private TupleIterator iter;

    public SortMergeJoin(JoinPredicate jp, OpIterator child1, OpIterator child2)
            throws TransactionAbortedException, DbException {
        super(jp, child1, child2);
        final int tuple1Num = blockCacheSize / child1.getTupleDesc().getSize();
        final int tuple2Num = blockCacheSize / child2.getTupleDesc().getSize();

        // build cache block
        this.block1         = new Tuple[tuple1Num];
        this.block2         = new Tuple[tuple2Num];

        final int field1    = jp.getField1();
        final int field2    = jp.getField2();
        this.lt             = new JoinPredicate(field1, Predicate.Op.LESS_THAN, field2);
        this.eq             = new JoinPredicate(field1, Predicate.Op.EQUALS, field2);

        // doJoin and cache
        this.iter           = doJoin();
        this.iter.open();
    }

    @Override
    public Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (!iter.hasNext()) {
            return null;
        }
        return iter.next();
    }


    public TupleIterator doJoin() {
        final List<Tuple> tupleList = new ArrayList<>();

        // fetch child1
        try {
            child1.rewind();
            while (child1.hasNext()) {
                int end1 = fetchTuples(child1, block1);
                // Fetch each block of child2, and do merge join
                child2.rewind();
                while (child2.hasNext()) {
                    int end2 = fetchTuples(child2, block2);
                    mergeJoin(tupleList, end1, end2);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error happen when sort merge join:" + e.getMessage());
        }
        Arrays.fill(block1, null);
        Arrays.fill(block2, null);
        return new TupleIterator(super.td, tupleList);
    }

    private void mergeJoin(final List<Tuple> tupleList, int end1, int end2) {
        // 1.Sort each block
        final int field1 = super.jp.getField1();
        final int field2 = super.jp.getField2();
        sortTuples(block1, field1, end1);
        sortTuples(block2, field2, end2);

        // 2.Join
        int index1 = 0, index2 = 0;
        final Predicate.Op op = super.jp.getOperator();
        switch (op) {
            case EQUALS: {
                while (index1 < end1 && index2 < end2) {
                    final Tuple lTuple = block1[index1];
                    final Tuple rTuple = block2[index2];
                    if (eq.filter(lTuple, rTuple)) {
                        // If equal , we should find the right boundary that equal to lTuple in block1 and rTuple in block2
                        final JoinPredicate eq1 = new JoinPredicate(field1, Predicate.Op.EQUALS, field1);
                        final JoinPredicate eq2 = new JoinPredicate(field2, Predicate.Op.EQUALS, field2);
                        int begin1 = index1 + 1, begin2 = index2 + 1;
                        while (begin1 < end1 && eq1.filter(lTuple, block1[begin1]))
                            begin1++;
                        while (begin2 < end2 && eq2.filter(rTuple, block2[begin2]))
                            begin2++;
                        for (int i = index1; i < begin1; i++) {
                            for (int j = index2; j < begin2; j++) {
                                tupleList.add(mergeTuple(block1[i], block2[j], super.td));
                            }
                        }
                        index1 = begin1;
                        index2 = begin2;
                    } else if (lt.filter(lTuple, rTuple)) {
                        index1++;
                    } else {
                        index2++;
                    }
                }
                return;
            }
            case LESS_THAN:
            case LESS_THAN_OR_EQ: {
                while (index1 < end1) {
                    final Tuple lTuple = block1[index1++];
                    while (index2 < end2 && !super.jp.filter(lTuple, block2[index2]))
                        index2++;
                    while (index2 < end2) {
                        final Tuple rTuple = block2[index2++];
                        tupleList.add(mergeTuple(lTuple, rTuple, td));
                    }
                }
                return;
            }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ: {
                while (index1 < end1) {
                    final Tuple lTuple = block1[index1++];
                    while (index2 < end2 && super.jp.filter(lTuple, block2[index2]))
                        index2++;
                    for (int i = 0; i < index2; i++) {
                        final Tuple rTuple = block2[i];
                        tupleList.add(mergeTuple(lTuple, rTuple, super.td));
                    }
                }
            }
        }
    }

    private void sortTuples(Tuple[] tuples, int field, int len) {
        final JoinPredicate lt = new JoinPredicate(field, Predicate.Op.LESS_THAN, field);
        final JoinPredicate gt = new JoinPredicate(field, Predicate.Op.GREATER_THAN, field);
        Arrays.sort(tuples, 0, len, (o1, o2) -> {
            if (lt.filter(o1, o2)) {
                return -1;
            }
            if (gt.filter(o1, o2)) {
                return 1;
            }
            return 0;
        });
    }

    private int fetchTuples(final OpIterator child, final Tuple[] tuples) throws Exception {
        int i = 0;
        Arrays.fill(tuples, null);
        while (child.hasNext() && i < tuples.length) {
            final Tuple next = child.next();
            if (next != null) {
                tuples[i++] = next;
            }
        }
        return i;
    }
}
