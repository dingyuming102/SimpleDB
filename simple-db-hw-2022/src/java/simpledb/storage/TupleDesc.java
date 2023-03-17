package simpledb.storage;

import simpledb.common.Type;
import simpledb.util.IteratorWrapper;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 *
 * TupleDesc 表示「表头」，描述一张表内的每一行「元组/记录」, 是一个"描述"。
 * TupleDesc 中有一个 TDItem 容器，而 TDItem 描述一个「列/属性/字段」, TDItem 也是一个"描述"。
 * 一个「表头」由一组「列/属性/字段」组合而成。所以一个「表头」的本质就是一系列描述的组合。
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A help class to facilitate organizing the information of each field
     *
     * TDItem 描述一个 列/属性/字段。
     * 类的原名是 TDItem，但个人感觉不形象。我感觉叫FieldDesc更好一些。
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type       fieldType;

        /**
         * The name of the field
         */
        public final String     fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType);
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final TDItem[]      fieldDescArr;
    private final int           byteSize;

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // DONE
//        return new Iterator<TDItem>() {
//            int idx = 0;
//            @Override
//            public boolean hasNext() {
//                return idx < fieldDescArr.length;
//            }
//            @Override
//            public TDItem next() {
//                if (hasNext()) {
//                    return fieldDescArr[idx++];
//                } else {
//                    throw new NoSuchElementException();
//                }
//            }
//            @Override
//            public void remove() {
//                fieldDescArr[idx] = null;
//            }
//        };
        return new IteratorWrapper<TDItem>(fieldDescArr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // Done
        // assert typeAr.length == fieldAr.length;
        fieldDescArr    = new TDItem[typeAr.length];
        int size = 0;
        for (int i = 0; i < typeAr.length; i++) {
            fieldDescArr[i] = new TDItem(typeAr[i], fieldAr[i]);
            size += typeAr[i].getLen();
        }
        byteSize        = size;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // Done
        this(typeAr, new String[typeAr.length]);
    }

    private TupleDesc(TupleDesc td1, TupleDesc td2) {
        fieldDescArr = new TDItem[td1.numFields() + td2.numFields()];
        System.arraycopy(td1.fieldDescArr, 0, fieldDescArr, 0, td1.numFields());
        System.arraycopy(td2.fieldDescArr, 0, fieldDescArr, td1.numFields(), td2.numFields());
        byteSize = td1.byteSize + td2.byteSize;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // Done
        return fieldDescArr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // Done
        if (i < 0 || i >= fieldDescArr.length) {
            throw new NoSuchElementException();
        }
        return fieldDescArr[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // Done
        if (i < 0 || i >= fieldDescArr.length) {
            throw new NoSuchElementException();
        }
        return fieldDescArr[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        // DONE
        if (name == null) {
            throw new NoSuchElementException();
        }

        for (int i = 0; i < fieldDescArr.length; i++) {
            if (fieldDescArr[i].fieldName == null) {
                continue;
            }
            if (name.equals(fieldDescArr[i].fieldName)) {
                return i;
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // Done
        return byteSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // DONE
        return new TupleDesc(td1, td2);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        // Done
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        return Arrays.equals(fieldDescArr, tupleDesc.fieldDescArr);
    }

    @Override
    public int hashCode() {
        // Done
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Arrays.hashCode(fieldDescArr);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // Done
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields() - 1; i++) {
            sb.append(fieldDescArr[i].toString());
            sb.append(',');
        }
        sb.append(fieldDescArr[fieldDescArr.length - 1].toString());
        return sb.toString();
    }
}
