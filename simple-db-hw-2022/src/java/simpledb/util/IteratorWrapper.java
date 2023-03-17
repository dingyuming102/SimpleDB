package simpledb.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class IteratorWrapper<T> implements Iterator<T> {
    private T[] objects;
    private int idx;

    public IteratorWrapper(final T[] objects) {
        this.objects    = objects;
        this.idx        = -1;
    }

    @Override
    public boolean hasNext() {
        return idx + 1 < objects.length;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return objects[++idx];
    }
    @Override
    public void remove() {
//        throw new UnsupportedOperationException("unimplemented");
        if (idx < 0) {
            throw new IllegalStateException();
        }
        objects[idx] = null;
    }
}
