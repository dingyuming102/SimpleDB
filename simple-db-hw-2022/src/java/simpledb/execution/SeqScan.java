package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.HeapFileIterator;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId       transactionId;
    private int                 tableId;
    private String              tableAlias;
    private DbFileIterator      baseIter;
    private TupleDesc           tupleDesc;

    private boolean             isOpenFlag;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // DONE
        this.transactionId  = tid;
        this.tableId        = tableid;
        this.tableAlias     = tableAlias;
        this.baseIter       = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        this.tupleDesc      = null;

        this.isOpenFlag     = false;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        // DONE
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // DONE
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // DONE
        this.tableId            = tableid;
        this.tableAlias         = tableAlias;
        this.tupleDesc          = null;
        final HeapFile dbFile   = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.baseIter           = new HeapFileIterator(transactionId, tableid, dbFile.numPages());

        this.isOpenFlag         = false;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // DONE
        baseIter.open();
        isOpenFlag = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // DONE
        if (this.tupleDesc != null) {
            return this.tupleDesc;
        }
        final TupleDesc     td      = Database.getCatalog().getTupleDesc(this.tableId);
        Type[]              typeAr  = new Type[td.numFields()];
        String[]            fieldAr = new String[td.numFields()];

        final String        prefix  = tableAlias == null ? "null": tableAlias;
        for (int i = 0; i < td.numFields(); i++) {
            typeAr[i] = td.getFieldType(i);
            String fieldName = td.getFieldName(i);
            if (fieldName == null) {
                fieldName = "null";
            }
            fieldAr[i] = prefix + "." + fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // DONE
        if (!isOpenFlag) {
            throw new IllegalStateException("The iterator is NOT open.");
        }
        return baseIter.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        // DONE
        if (!isOpenFlag) {
            throw new IllegalStateException("The iterator is NOT open.");
        }

//        final Tuple next = iter.next();
//        final Tuple res = new Tuple(getTupleDesc());
//        for (int i = 0; i < next.getTupleDesc().numFields(); i++) {
//            res.setField(i, next.getField(i));
//            res.setRecordId(next.getRecordId());
//        }
//        return res;
        return baseIter.next();
    }

    public void close() {
        // DONE
//        写了过不了JoinTest
//        if (!isOpenFlag) {
//            throw new IllegalStateException("The iterator is NOT open.");
//        }
        isOpenFlag = false;
        baseIter.close();
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        // DONE
        if (!isOpenFlag) {
            throw new IllegalStateException("The iterator is NOT open.");
        }
        baseIter.rewind();
    }
}
