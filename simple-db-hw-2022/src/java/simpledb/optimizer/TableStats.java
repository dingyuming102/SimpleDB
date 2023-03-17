package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats>  statsMap        = new ConcurrentHashMap<>();
    static final int                                        IOCOSTPERPAGE   = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int            NUM_HIST_BINS = 100;

    private int                 tableId;
    private TupleDesc           td;
    private final Histogram[]   histograms;     // FieldId -> histogram (String or Integer)
    private int                 tupleNum;
    private int                 pageNum;
    private int                 ioCostPerPage;


    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // DONE
        this.tableId            = tableid;
        final HeapFile table    = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.td                 = table.getTupleDesc();
        this.histograms         = new Histogram[td.numFields()];
        this.tupleNum           = 0;

        // init mins and maxs
        int[] mins = new int[td.numFields()];
        int[] maxs = new int[td.numFields()];
        Arrays.fill(mins, Integer.MAX_VALUE);
        Arrays.fill(maxs, Integer.MIN_VALUE);

        // Compute the minimum and maximum values for every attribute in the table
        // (by scanning it once).
        SeqScan scan = new SeqScan(new TransactionId(), tableid, "");
        try {
            scan.open();
            while (scan.hasNext()) {
                this.tupleNum++;
                Tuple tup = scan.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int value = ((IntField) tup.getField(i)).getValue();
                        mins[i] = Math.min(mins[i], value);
                        maxs[i] = Math.max(maxs[i], value);
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }


        // set histograms
        for (int i = 0; i < td.numFields(); ++i) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                histograms[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
            } else {
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }


        // Scan the table again,
        // selecting out all of fields of all of the tuples and
        // using them to populate the counts of the buckets in each histogram
        try {
            scan.rewind();
            while (scan.hasNext()) {
                Tuple tup = scan.next();
                for (int i = 0; i < td.numFields(); ++i) {
                    if (tup.getField(i).getType() == Type.INT_TYPE) {
                        histograms[i].addValue(((IntField) tup.getField(i)).getValue());
                    } else {
                        // String Type
                        histograms[i].addValue(((StringField) tup.getField(i)).getValue());
                    }
                }
            }

        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            scan.close();
        }

        this.pageNum        = table.numPages();
        this.ioCostPerPage  = ioCostPerPage;
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // DONE
        return pageNum * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // DONE
        return (int) (this.tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // DONE
        assert field >= 0 && field < histograms.length;
        switch (td.getFieldType(field)) {
            case INT_TYPE: {
                return histograms[field].estimateSelectivity(op, ((IntField) constant).getValue());
            }
            case STRING_TYPE: {
                return histograms[field].estimateSelectivity(op, ((StringField) constant).getValue());
            }
        }

        return 0.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // DONE
        return tupleNum;
    }

}
