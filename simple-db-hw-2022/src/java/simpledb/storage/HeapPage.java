package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    private final HeapPageId        pid;
    private final TupleDesc         td;
    private final int               numSlots;
    private final byte[]            header;
    private final Tuple[]           tuples;

    private byte[]                  oldData;
    private final Byte              oldDataLock = (byte) 0;

    private volatile boolean        dirty = false;
    private volatile TransactionId  dirtier;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid            = id;
        this.td             = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots       = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        this.header         = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();
        }

        this.tuples         = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        } finally {
            dis.close();
        }

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // DONE
        // floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
        // 高可读性，低性能
        // return (int) Math.floor((BufferPool.getPageSize() * 8.0) / (td.getSize() * 8.0 + 1.0));
        // 中可读性，中性能
        // return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);

        // 低可读性，高性能
        return (BufferPool.getPageSize() << 3) / ((td.getSize() << 3) + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // DONE
        // ceiling(no. tuple slots / 8)
        // 可读性高，性能低
        // return (int) Math.ceil(getNumTuples() / 8.0);
        // 中可读性，中性能
        // return (getNumTuples() + 7) / 8;

        // 低可读性，高性能
        return (numSlots + 7) >>> 3;
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // DONE
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // DONE
        // not necessary for lab1
        assert t != null;
        final RecordId      recordId    = t.getRecordId();
        assert recordId != null;
        final HeapPageId    hPageId     = (HeapPageId) recordId.getPageId();
        final int           slotId      = recordId.getTupleNumber();
        if (!pid.equals(hPageId)) {
            throw new DbException("Page ID not match. This tuple is not on this page.");
        }
        if (!isSlotUsed(slotId) || slotId < 0 || slotId >= numSlots) {
            throw new DbException("The tuple slot is already empty.");
        }
        markSlotUsed(slotId, false);
        tuples[slotId] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // DONE
        // not necessary for lab1
        assert t != null;
        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("Tuple desc is mismatch.");
        }
//        for (int i = 0; i < numSlots; i++) {
//            if (isSlotUsed(i)) {
//                continue;
//            }
//            t.setRecordId(new RecordId(pid, i));
//            tuples[i] = t;
//            markSlotUsed(i, true);
//            return;
//        }
        for (int i = 0; i < header.length; i++) {
            if ((header[i] ^ 0b11111111) == 0) {
                continue;
            }
            for (int j = 0, slotId = i << 3; j < 8 && slotId < numSlots; j++, slotId++) {
                if (isSlotUsed(slotId)) {
                    continue;
                }
                t.setRecordId(new RecordId(pid, slotId));
                tuples[slotId] = t;
                markSlotUsed(slotId, true);
                return;
            }
        }
        throw new DbException("The page is full. No empty slots.");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // DONE
        // not necessary for lab1
        this.dirtier = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // DONE
        // Not necessary for lab1
        return dirtier;
    }

    /**
     * Returns the number of unused (i.e., empty) slots on this page.
     */
    public int getNumUnusedSlots() {
        // DONE
        // 位运算不用在乎性能
        // 高可读，低性能
        int unusedCount = 0;
        for (int i = 0; i < numSlots; i ++ ) {
            if (!isSlotUsed(i)) {
                unusedCount++;
            }
        }
        return unusedCount;

        // 低可读，高性能
//        int unusedCount = numSlots;
//        for (byte b: header) {
//            byte mask = 0b1;
//            for (int i = 0; i < 8; i++) {
//                unusedCount -= b & mask;
//                mask <<= 1;
//            }
//        }
//        return unusedCount;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // done
        // 高可读，低性能
//        int index = i / 8;
//        int offset = i % 8;
//        byte mask = (byte)(0b1 << (offset));
//        byte b = header[index];
//        int res = ((b & mask) >>> (offset));
//        return res == 1;

        // 低可读，高性能
        // header[i>>>3],       i>>>3 equals to i/8, get the corresponding byte
        // 0b1 << (i & 0b111),  i&0b111 equals to i%8, get the corresponding offset bit in the byte
        return (header[i>>>3] & (0b1 << (i & 0b111))) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean isUsed) {
        // DONE
        // not necessary for lab1
        if (isUsed) {
            header[i >>> 3] |= (1 << (i & 0b111));
        } else {
            header[i >>> 3] &= ~(1 << (i & 0b111));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     *         (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // DONE
        return new Iterator<Tuple>() {
            private int idx = -1;

            @Override
            public boolean hasNext() {
                while (idx + 1 < numSlots && !isSlotUsed(idx + 1)) {
                    idx++;
                }
                return idx + 1 < numSlots;
            }
            @Override
            public Tuple next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return tuples[++idx];
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}

