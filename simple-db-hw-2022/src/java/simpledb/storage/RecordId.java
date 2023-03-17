package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId    pid;
    private final int       tupleNo;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // DONE
        this.pid        = pid;
        this.tupleNo    = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // DONE
        return tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // DONE
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // DONE
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordId other = (RecordId) o;
        return this.tupleNo == other.tupleNo && Objects.equals(this.pid, other.pid);
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // DONE
        return Objects.hash(pid, tupleNo);
    }

}
