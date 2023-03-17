package simpledb.storage;

import simpledb.util.IteratorWrapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 *
 *
 *
 *  id(int)  name(string)  sex(string)
 *  1           xxx         m
 *  2           yyy         f
 *  那么(1, xxx, m)就是由一组Field组成的一个Tuple，然后TupleDesc是(id(int) name(string) sex(string))。
 *  Tuple = 描述(表头) + 数据载体(一系列 Field 的组合)。
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc   td;
    private RecordId    rid;
    private Field[]     fieldArr;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // DONE
        this.td         = td;
        this.fieldArr   = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // DONE
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // DONE
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // DONE
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // DONE
        fieldArr[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // DONE
        return fieldArr[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // Done
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldArr.length - 1; i++) {
            sb.append(fieldArr[i].toString());
            sb.append('\t');
        }
        sb.append(fieldArr[fieldArr.length - 1].toString());
        sb.append('\n');
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // Done
//        return new Iterator<Field>() {
//            int idx = 0;
//            @Override
//            public boolean hasNext() {
//                return idx < fieldArr.length;
//            }
//            @Override
//            public Field next() {
//                if (hasNext()) {
//                    return fieldArr[idx++];
//                } else {
//                    throw new NoSuchElementException();
//                }
//            }
//            @Override
//            public void remove() {
//                fieldArr[idx] = null;
//            }
//        };
        return new IteratorWrapper<Field>(fieldArr);
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // Done
        this.td = td;
    }
}
