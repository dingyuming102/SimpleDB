package simpledb.storage;

import simpledb.common.Type;
import simpledb.execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for values of fields in tuples in SimpleDB.
 *
 * 数据库中, cell 和 接口 Field 对应。它是单一元素的基本「数据载体」。
 *          Field 接口的各种实现类就是各种数据类型的「载体」。
 */
public interface Field extends Serializable {
    /**
     * Write the bytes representing this field to the specified
     * DataOutputStream.
     *
     * @param dos The DataOutputStream to write to.
     * @see DataOutputStream
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * Compare the value of this field object to the passed in value.
     *
     * @param op    The operator
     * @param value The value to compare this Field to
     * @return Whether or not the comparison yields true.
     */
    boolean compare(Predicate.Op op, Field value);

    /**
     * Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
     *
     * @return type of this field
     */
    Type getType();

    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    int hashCode();

    boolean equals(Object field);

    String toString();
}
