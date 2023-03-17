package simpledb.transaction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author
 * @create 2023-03-13 1:37
 *
 * inner class, page-level lock
 */
public class PageLock {
    // 内部实现：tidSet可能为空，但是不会为null。
    // 外部实现：所有Lock的tidSet不可能为空。因为tidSet为空时，该Lock从数据结构中remove。
    private volatile Set<TransactionId> tidSet;
    private volatile LockType locktype;


    public PageLock(LockType locktype) {
        this.tidSet     = ConcurrentHashMap.newKeySet();
        this.locktype   = locktype;
    }

    public synchronized void addTid(TransactionId tid) {
        tidSet.add(tid);
    }

    public synchronized void setLockType(LockType locktype) {
        this.locktype = locktype;
    }

    public synchronized Set<TransactionId> getTidSet() { return tidSet; }

    public synchronized LockType getLockType() { return locktype;}

    public synchronized void removeTid(TransactionId tid) { tidSet.remove(tid); }

    public synchronized int size() { return tidSet.size(); }

    public synchronized boolean isEmpty() { return tidSet.isEmpty(); }

    public synchronized boolean containsTid(TransactionId tid) { return tidSet.contains(tid); }
}
