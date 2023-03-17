package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    static class AggOperation {
        public static int min(List<IntField> gbList) {
            return gbList.stream().mapToInt(IntField::getValue).min().getAsInt();
//            return Collections.min(gbList, (field1, field2) -> field1.getValue() - field2.getValue()).getValue();
        }

        public static int max(List<IntField> gbList) {
            return gbList.stream().mapToInt(IntField::getValue).max().getAsInt();
//            return Collections.max(gbList, (field1, field2) -> field1.getValue() - field2.getValue()).getValue();
        }

        public static int sum(List<IntField> gbList) {
            return gbList.stream().mapToInt(IntField::getValue).sum();
        }

        public static int avg(List<IntField> gbList) {
            return (int) gbList.stream().mapToDouble(IntField::getValue).average().getAsDouble();
//            return (int) 1.0 * sum(gbList) / count(gbList);
        }

        public static int count(List<IntField> gbList) {
            return gbList.size();
        }
    }

    // Group by field
    private int                         gbfield;
    private Type                        gbfieldtype;
    // Aggregation field
    private int                         agfield;
    // Aggregation operation
    private Op                          aop;

    private TupleDesc                   td;
    private Map<Field, List<IntField>>  groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // DONE
        this.gbfield        = gbfield;
        this.gbfieldtype    = gbfieldtype;
        this.agfield        = afield;
        this.aop            = what;

        this.groupMap       = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // DONE
        if (this.td == null) {
            this.td = buildTupleDesc(tup.getTupleDesc());
        }
        final Field     gbFIELD = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        final IntField  aFIELD  = (IntField) tup.getField(agfield);

        List<IntField> gbList = groupMap.getOrDefault(gbFIELD, new LinkedList<>());
        gbList.add(aFIELD);
        groupMap.put(gbFIELD, gbList);
    }

    public TupleDesc buildTupleDesc(final TupleDesc originTd) {
        Type[]      types;
        String[]    names;
        if (gbfield == NO_GROUPING) {
            types = new Type[] { Type.INT_TYPE };
            names = new String[] { aop.toString() + '(' + originTd.getFieldName(agfield) + ')' };
        } else {
            types = new Type[] { gbfieldtype, Type.INT_TYPE };
            names = new String[] { originTd.getFieldName(gbfield), aop.toString() + '(' + originTd.getFieldName(agfield) + ')' };
        }

        return new TupleDesc(types, names);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // DONE
        final List<Tuple> tuples = new LinkedList<>();
        if (gbfield != NO_GROUPING) {
            groupMap.forEach((key, gbList) -> {
                final Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(callByReflection(gbList)));
                tuples.add(tuple);
            });
        } else {
            final Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(callByReflection(groupMap.get(null))));
            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }

    public int callByReflection(List<IntField> gbList) {
        int aVal = 0;
        try {
            Method staticMethod = AggOperation.class.getMethod(aop.name().toLowerCase(), List.class);
            aVal = (int) staticMethod.invoke(null, gbList);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("NOT possible to reach");
        }
        return aVal;
    }

}
