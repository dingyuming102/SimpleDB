package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.BufferPool;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author
 * @create 2023-03-12 17:37
 *
 * 管理事务-锁的业务逻辑
 */
public class LockManager {

    private volatile DependencyGraph depGraph;

    private volatile ConcurrentHashMap<PageId, LockType> page2Lock;

    // 不能设置为单例，因为测试代码会resetBufferPoll，
    // 如果设置为单例，则LockManager不会被重置。
    public LockManager() {
        this.depGraph = new DependencyGraph<PageId, TransactionId>();
        this.page2Lock = new ConcurrentHashMap<>();
    }


    /**
     * @return if tid holds a lock on pid
     * */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            return depGraph.holdsTP(tid, pid);
        }
    }

    /**
     * tid adds a lock on pid
     *
     *
     * @param tid
     * @param pid
     * @param perm
     *            decides the type of lock
     */
    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        while (true) {
            synchronized (pid) {
                if (perm == Permissions.READ_ONLY ? acquireSLock(tid, pid) : acquireXLock(tid, pid)) {
                    break;
                }

                try {

                    depGraph.addDependency(pid, tid);
                    if (depGraph.hasCycle()) {
                        // Exist a Cycle. Abort this transaction.
                        System.out.println("Transaction " + tid.getId() + " Aborted!");
                        throw new TransactionAbortedException();
                    }
                } finally {
                    depGraph.removeDependency(pid, tid);
                }
            }
        }
    }

    private synchronized boolean acquireSLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            LockType lockState = page2Lock.getOrDefault(pid, LockType.NoLock);
            if (lockState == LockType.NoLock) {
                page2Lock.put(pid, LockType.SLock);
                depGraph.addDependency(pid, tid);
                return true;
            } else if (lockState == LockType.SLock) {
                depGraph.addDependency(pid, tid);
                return true;
            } else if (lockState == LockType.XLock && depGraph.holdsTP(tid, pid)) {
                return true;
            }
            return false;
        }
    }

    private synchronized boolean acquireXLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            LockType lockState = page2Lock.getOrDefault(pid, LockType.NoLock);
            if (lockState == LockType.NoLock) {
                page2Lock.put(pid, LockType.XLock);
                depGraph.addDependency(pid, tid);
                return true;
            } else if (lockState == LockType.SLock) {
                if (depGraph.getTSetByP(pid).contains(tid) && depGraph.getTSetByP(pid).size() == 1) {
                    // upgrade the SLock
                    page2Lock.put(pid, LockType.XLock);
                    return true;
                }
            } else if (lockState == LockType.XLock && depGraph.getTSetByP(pid).contains(tid)) { // tid already holds an XLock on pid
                return true;
            }
        }
        // other transaction holds SLock or XLock on pid
        return false;
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            if (!holdsLock(tid, pid)) {
                System.out.println("Release Lock FAIL: TransactionID: " + tid.getId() +
                        " DO NOT lock " + "PageId: " + pid.getPageNumber());
                return;
            }
            depGraph.removeDependency(pid, tid);
            if (!depGraph.containsP(pid) || depGraph.isEmptyP(pid)) {
                page2Lock.remove(pid);
            }
        }
    }

    // 传入的tid有可能为null
    public synchronized void releaseLock(TransactionId tid) {
        if (depGraph.containsT(tid)) {
            Set<PageId> pidSet = depGraph.getPSetByT(tid);
            synchronized (pidSet) {
                for (PageId pid : new HashSet<>(pidSet)) {
                    synchronized (pid) {
                        releaseLock(tid, pid);
                    }
                }
            }
        }
    }

    /*
     * Return the pages which are locked by tid
     * If tid holds no lock on any page, return NULL
     */
    public Set<PageId> getLockedPage(TransactionId tid) {
        return depGraph.getPSetByT(tid);
    }








//    private volatile DependencyGraph depGraph;
//
//    // 不能设置为单例，因为测试代码会resetBufferPoll，
//    // 如果设置为单例，则LockManager不会被重置。
//    public LockManager() {
//        this.depGraph = new DependencyGraph();
//    }
//
//    public LockType getLockType(PageId pid) {
//        synchronized (pid) {
//            return depGraph.getLockType(pid);
//        }
//    }
//
//    /**
//     * @return if tid holds a lock on pid
//     * */
//    public boolean holdsLock(TransactionId tid, PageId pid) {
//        synchronized (pid) {
//            return depGraph.holdsLock(tid, pid);
//        }
//    }
//
//    /**
//     * tid adds a lock on pid
//     *
//     *
//     * @param tid
//     * @param pid
//     * @param perm
//     *            decides the type of lock
//     */
//    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
//        while (true) {
//            // 这里有毛病，acquire不到锁，意味着有线程正在读/写pid和相应page，这个时候再读写很有可能发生问题
//            synchronized (pid) {
//                try {
//                    if(perm == Permissions.READ_ONLY ? acquireSLock(tid, pid) : acquireXLock(tid, pid)) {
//                        break;
//                    }
//                } catch (NullPointerException e) {
//                    System.out.println("NullPointerException!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                    throw new TransactionAbortedException();
//                }
//                try {
//
//                    depGraph.addDependency(pid, tid, perm == Permissions.READ_ONLY ? LockType.SLock : LockType.XLock);
//                    if (depGraph.hasCycle()) {
//                        // Exist a Cycle. Abort this transaction.
//                        System.out.println("Transaction " + tid.getId() + " Aborted!");
//                        throw new TransactionAbortedException();
//                    }
//                } catch (ConcurrentModificationException e) {
//                    continue;
//                } catch (NullPointerException e) {
//                    System.out.println("NullPointerException!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                    throw new TransactionAbortedException();
//                } finally {
//                    depGraph.removeDependency(pid, tid);
//                }
//            }
//        }
//    }
//
//    private boolean acquireSLock(TransactionId tid, PageId pid) {
//        synchronized (pid) {
//            if (holdsLock(tid, pid)) {
//                return true;
//            }
//            LockType lockState = getLockType(pid);
//            if (lockState == LockType.NoLock) {
//                depGraph.addDependency(pid, tid, LockType.SLock);
//                return true;
//            } else if (lockState == LockType.SLock) {
//                depGraph.addDependency(pid, tid, LockType.SLock);
//                return true;
//            } else {
//                return false;
//            }
//        }
//    }
//
//    private boolean acquireXLock(TransactionId tid, PageId pid) {
//        synchronized (pid) {
//            LockType lockState = getLockType(pid);
//            if (lockState == LockType.NoLock) {
//                depGraph.addDependency(pid, tid, LockType.XLock);
//                return true;
//            } else if (lockState == LockType.SLock) {
//                if (holdsLock(tid, pid) && depGraph.getLockByPage(pid).size() == 1) {
//                    // upgrade the SLock
//                    depGraph.getLockByPage(pid).setLockType(LockType.XLock);
//                    return true;
//                }
//            } else if (holdsLock(tid, pid)) { // tid already holds an XLock on pid
//                return true;
//            }
//        }
//        // other transaction holds SLock or XLock on pid
//        return false;
//    }
//
//    public void releaseLock(TransactionId tid, PageId pid) {
//        synchronized (pid) {
//            if (!holdsLock(tid, pid)) {
//                System.out.println("Release Lock FAIL: TransactionID: " + tid.getId() +
//                        " DO NOT lock " + "PageId: " + pid.getPageNumber());
//                return;
//            }
//            depGraph.removeDependency(pid, tid);
//        }
//    }
//
//    // 传入的tid有可能为null
//    public void releaseLock(TransactionId tid) {
//        if (depGraph.containsTxn(tid)) {
//            Set<PageId> pidSet = depGraph.getLockedPageByTxn(tid);
//            synchronized (pidSet) {
//                for (PageId pid : new HashSet<>(pidSet)) {
//                    synchronized (pid) {
//                        releaseLock(tid, pid);
//                    }
//                }
//            }
//        }
//    }
//
//    /*
//     * Return the pages which are locked by tid
//     * If tid holds no lock on any page, return NULL
//     */
//    public Set<PageId> getLockedPage(TransactionId tid) {
//        return depGraph.getLockedPageByTxn(tid);
//    }








//    public synchronized boolean hasCycle() {
//        // Create a set of unvisited transactions
//        Set<TransactionId> unvisitedTransactionSet = new HashSet<>(tidToLockedPage.keySet());
//
//        // Visit each transaction
//        while (!unvisitedTransactionSet.isEmpty()) {
//            TransactionId tid = unvisitedTransactionSet.iterator().next();
//            if (hasCycleHelper(tid, new HashSet<>())) {
//                return true;
//            }
//            unvisitedTransactionSet.remove(tid);
//        }
//
//        return false;
//    }
//
//    private synchronized boolean hasCycleHelper(TransactionId tid, Set<TransactionId> visitedTransactionSet) {
//        // Check if this transaction has already been visited
//        if (visitedTransactionSet.contains(tid)) {
//            return true;
//        }
//
//        // Add the transaction to the set of visited transactions
//        visitedTransactionSet.add(tid);
//
//        // Check for cycles in the dependency graph
//        if (tidToLockedPage.containsKey(tid)) {
//            for (PageId pid : tidToLockedPage.get(tid)) {
//                if (pageIdToLock.containsKey(pid)) {
//                    for (TransactionId dependentTid : pageIdToLock.get(pid).getTidSet()) {
//                        if (hasCycleHelper(dependentTid, visitedTransactionSet)) {
//                            return true;
//                        }
//                    }
//                }
//            }
//        }
//
//        // Remove the transaction from the set of visited transactions
//        visitedTransactionSet.remove(tid);
//
//        return false;
//    }
//
//    public void addDependency(PageId pid, TransactionId tid, LockType lockType) {
//        // Add the transaction and page to the maps
//        synchronized (pid) {
//            pageIdToLock.computeIfAbsent(pid, k -> new PageLock(lockType)).addTid(tid);
//            tidToLockedPage.computeIfAbsent(tid, k -> Collections.synchronizedSet(new HashSet<>())).add(pid);
//        }
//    }
//
//    public void removeDependency(PageId pid, TransactionId tid) {
//        synchronized (pid) {
//            if (!holdsLock(tid, pid)) {
//                System.out.println("Remove Dependency FAIL: TransactionID: " + tid.getId() +
//                        "do NOT lock" + "PageId: " + pid.getPageNumber());
//                return;
//            }
//
//            // Remove the transaction from the set of transactions that depend on the page
//            Set<TransactionId> tidSet = pageIdToLock.get(pid).getTidSet();
//            pageIdToLock.get(pid).removeTid(tid);   // 因为holdsLock(tid, pid)，所以Lock一定不为null，而tidSet不会为空，更不为null。
//            if (pageIdToLock.get(pid).isEmpty()) {
//                pageIdToLock.remove(pid);
//            }
//
//
//
//            // Remove the page from the set of pages that the transaction depends on
//            Set<PageId> pidSet = tidToLockedPage.get(tid);
//            if (pidSet != null) {  // 数据结构中不存在此 TransactionId
//                pidSet.remove(pid);
//                if (pidSet.isEmpty()) {
//                    tidToLockedPage.remove(tid);
//                }
//            }
//        }
//    }
}
