package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Group by field
    private int                 gbfield;
    private Type                gbfieldtype;
    // Aggregation field
    private int                 agfield;
    // Aggregation operation
    private Op                  aop;

    private TupleDesc           td;
    private Map<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // DONE
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Err in StringAggregator: What != Count");
        }
        this.gbfield        = gbfield;
        this.gbfieldtype    = gbfieldtype;
        this.agfield        = afield;
        this.aop            = what;

        this.groupMap       = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // DONE
        if (this.td == null) {
            this.td = buildTupleDesc(tup.getTupleDesc());
        }
        final Field         gbFIELD = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        final StringField   aFIELD  = (StringField) tup.getField(agfield);
        groupMap.put(gbFIELD, groupMap.getOrDefault(gbFIELD, 0) + 1);
    }

    public TupleDesc buildTupleDesc(final TupleDesc originTd) {
        Type[]      types;
        String[]    names;
        if (gbfield == NO_GROUPING) {
            types = new Type[] { Type.INT_TYPE };
            names = new String[] { aop.toString() + '(' + originTd.getFieldName(agfield) + ')' };
        } else {
            types = new Type[] { gbfieldtype, Type.INT_TYPE };
            names = new String[] { originTd.getFieldName(gbfield), aop.toString()};
        }

        return new TupleDesc(types, names);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // DONE
        final List<Tuple> tuples = new LinkedList<>();
        if (gbfield != NO_GROUPING) {
            groupMap.forEach((key, cnt) -> {
                final Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(cnt));
                tuples.add(tuple);
            });
        } else {
            final Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(groupMap.get(null)));
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
