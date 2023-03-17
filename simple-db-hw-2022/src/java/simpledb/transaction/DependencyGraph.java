package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author
 * @create 2023-03-12 17:17
 *
 * 事务-页的图数据结构
 */
public class DependencyGraph<P, T> {

    private volatile ConcurrentHashMap<P, Set<T>> p2tMap;
    private volatile ConcurrentHashMap<T, Set<P>> t2pMap;

    public DependencyGraph() {
        this.p2tMap = new ConcurrentHashMap<>();
        this.t2pMap = new ConcurrentHashMap<>();
    }

    public boolean holdsTP(T t, P p) {
        return t2pMap.containsKey(t) && t2pMap.get(t).contains(p);

    }

    public boolean containsT(T t) { return t2pMap.containsKey(t); }

    public boolean containsP(P p) { return p2tMap.containsKey(p); }

    public boolean isEmptyP(P p) { return p2tMap.getOrDefault(p, Collections.emptySet()).isEmpty(); }

    public boolean isEmptyTxn(TransactionId t) { return t2pMap.getOrDefault(t, Collections.emptySet()).isEmpty(); }

    public Set<T> getTSetByP(P p) { return p2tMap.get(p); }

    public Set<P> getPSetByT(T t) { return t2pMap.get(t); }

    public synchronized void addDependency(P p, T t) {
        // Add the transaction and page to the maps
        p2tMap.computeIfAbsent(p, k -> ConcurrentHashMap.newKeySet()).add(t);
        t2pMap.computeIfAbsent(t, k -> ConcurrentHashMap.newKeySet()).add(p);

    }

    public void removeDependency(P p, T t) {
        // Remove the transaction from the set of transactions that depend on the page
        p2tMap.computeIfPresent(p, (k, v) -> {
            v.remove(t);
            return v.isEmpty() ? null : v;
        });

        // Remove the page from the set of pages that the transaction depends on
        t2pMap.computeIfPresent(t, (k, v) -> {
            v.remove(p);
            return v.isEmpty() ? null : v;
        });
    }


    public boolean hasCycle() {
        // Create a set of unvisited transactions
        Set<T> unvisitedTransactionSet = new HashSet<T>(t2pMap.keySet());

        // Visit each transaction
        while (!unvisitedTransactionSet.isEmpty()) {
            T t = unvisitedTransactionSet.iterator().next();
            if (hasCycleHelper(t, new HashSet<>())) {
                return true;
            }
            unvisitedTransactionSet.remove(t);
        }

        return false;
    }

    private synchronized boolean hasCycleHelper(T tid, Set<T> visitedTransactionSet) {
        // Check if this transaction has already been visited
        if (visitedTransactionSet.contains(tid)) {
            return true;
        }

        // Add the transaction to the set of visited transactions
        visitedTransactionSet.add(tid);

        // Check for cycles in the dependency graph
        if (t2pMap.containsKey(tid)) {
            for (P p : t2pMap.get(tid)) {
                if (p2tMap.containsKey(p)) {
                    for (T dependentTid : p2tMap.get(p)) {
                        if (hasCycleHelper(dependentTid, visitedTransactionSet)) {
                            return true;
                        }
                    }
                }
            }
        }

        // Remove the transaction from the set of visited transactions
        visitedTransactionSet.remove(tid);

        return false;
    }

}








//public class DependencyGraph {
//
//    private volatile ConcurrentHashMap<PageId, PageLock> pid2Lock;
//    // 外部实现：不存在 TransactionId -> Empty Set<PageId>。换言之，如果TransactionId不在数据结构中，则get返回null.
//    private volatile ConcurrentHashMap<TransactionId, Set<PageId>> tid2LockedPage;
//
//    public DependencyGraph() {
//        this.pid2Lock       = new ConcurrentHashMap<>(BufferPool.getPageSize());
//        this.tid2LockedPage = new ConcurrentHashMap<>();
//    }
//
//    public LockType getLockType(PageId pid) {
//        synchronized (pid) {
//            if (!pid2Lock.containsKey(pid) || pid2Lock.get(pid).size() == 0) {
//                return LockType.NoLock;
//            }
//            return pid2Lock.get(pid).getLockType();
//        }
//    }
//
//    public boolean holdsLock(TransactionId tid, PageId pid) {
//        synchronized (pid) {
//            return pid2Lock.containsKey(pid) && pid2Lock.get(pid).containsTid(tid);
//        }
//    }
//
//    public boolean containsTxn(TransactionId tid) { return tid2LockedPage.containsKey(tid); }
//
//    public PageLock getLockByPage(PageId pid) { return pid2Lock.get(pid); }
//
//    public Set<PageId> getLockedPageByTxn(TransactionId tid) { return tid2LockedPage.get(tid); }
//
//    public void addDependency(PageId pid, TransactionId tid, LockType lockType) {
//        // Add the transaction and page to the maps
//        synchronized (pid) {
//            pid2Lock.computeIfAbsent(pid, k -> new PageLock(lockType)).addTid(tid);
//            tid2LockedPage.computeIfAbsent(tid, k -> ConcurrentHashMap.newKeySet()).add(pid);
//        }
//    }
//
//    public void removeDependency(PageId pid, TransactionId tid) {
//        synchronized (pid) {
//            if (!holdsLock(tid, pid)) {
//                System.out.println("Remove Dependency FAIL: TransactionID: " + tid.getId() +
//                        " DO NOT lock" + "PageId: " + pid.getPageNumber());
//                return;
//            }
//
//            // Remove the transaction from the set of transactions that depend on the page
//            pid2Lock.computeIfPresent(pid, (k, v) -> {
//                v.removeTid(tid);
//                return v.isEmpty() ? null : v;
//            });
//
//
//
//            // Remove the page from the set of pages that the transaction depends on
//            tid2LockedPage.computeIfPresent(tid, (k, v) -> {
//                v.remove(pid);
//                return v.isEmpty() ? null : v;
//            });
//        }
//    }
//
//    public boolean hasCycle() {
//        // Create a set of unvisited transactions
//        Set<TransactionId> unvisitedTransactionSet = new HashSet<>(tid2LockedPage.keySet());
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
//    private boolean hasCycleHelper(TransactionId tid, Set<TransactionId> visitedTransactionSet) {
//        // Check if this transaction has already been visited
//        if (visitedTransactionSet.contains(tid)) {
//            return true;
//        }
//
//        // Add the transaction to the set of visited transactions
//        visitedTransactionSet.add(tid);
//
//        // Check for cycles in the dependency graph
//        if (tid2LockedPage.containsKey(tid)) {
//            for (PageId pid : tid2LockedPage.get(tid)) {
//                if (pid2Lock.containsKey(pid)) {
//                    for (TransactionId dependentTid : pid2Lock.get(pid).getTidSet()) {
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
//}




//import java.util.Map;
//        import java.util.Set;
//        import java.util.HashSet;
//        import java.util.concurrent.ConcurrentHashMap;
//
//public class DependencyGraph {
//    private final Map<PageId, Set<TransactionId>> pageToTransactionsMap;
//    private final Map<TransactionId, Set<PageId>> transactionToPagesMap;
//
//    public DependencyGraph() {
//        pageToTransactionsMap = new ConcurrentHashMap<>();
//        transactionToPagesMap = new ConcurrentHashMap<>();
//    }
//
//    public synchronized void addDependency(PageId pageId, TransactionId transactionId) {
//        // Add the transaction and page to the maps
//        pageToTransactionsMap.computeIfAbsent(pageId, k -> new HashSet<>()).add(transactionId);
//        transactionToPagesMap.computeIfAbsent(transactionId, k -> new HashSet<>()).add(pageId);
//    }
//
//    public synchronized boolean hasCycle() {
//        // Create a set of unvisited transactions
//        Set<TransactionId> unvisitedTransactions = new HashSet<>(transactionToPagesMap.keySet());
//
//        // Visit each transaction
//        while (!unvisitedTransactions.isEmpty()) {
//            TransactionId transactionId = unvisitedTransactions.iterator().next();
//            if (hasCycleHelper(transactionId, new HashSet<>())) {
//                return true;
//            }
//            unvisitedTransactions.remove(transactionId);
//        }
//
//        return false;
//    }
//
//    private boolean hasCycleHelper(TransactionId transactionId, Set<TransactionId> visitedTransactions) {
//        // Check if this transaction has already been visited
//        if (visitedTransactions.contains(transactionId)) {
//            return true;
//        }
//
//        // Add the transaction to the set of visited transactions
//        visitedTransactions.add(transactionId);
//
//        // Check for cycles in the dependency graph
//        if (transactionToPagesMap.containsKey(transactionId)) {
//            for (PageId pageId : transactionToPagesMap.get(transactionId)) {
//                if (pageToTransactionsMap.containsKey(pageId)) {
//                    for (TransactionId dependentTransactionId : pageToTransactionsMap.get(pageId)) {
//                        if (hasCycleHelper(dependentTransactionId, visitedTransactions)) {
//                            return true;
//                        }
//                    }
//                }
//            }
//        }
//
//        // Remove the transaction from the set of visited transactions
//        visitedTransactions.remove(transactionId);
//
//        return false;
//    }
//
//    public synchronized void removeDependency(PageId pageId, TransactionId transactionId) {
//        // Remove the transaction from the set of transactions that depend on the page
//        Set<TransactionId> transactionIds = pageToTransactionsMap.get(pageId);
//        if (transactionIds != null) {
//            transactionIds.remove(transactionId);
//            if (transactionIds.isEmpty()) {
//                pageToTransactionsMap.remove(pageId);
//            }
//        }
//
//        // Remove the page from the set of pages that the transaction depends on
//        Set<PageId> pageIds = transactionToPagesMap.get(transactionId);
//        if (pageIds != null) {
//            pageIds.remove(pageId);
//            if (pageIds.isEmpty()) {
//                transactionToPagesMap.remove(transactionId);
//            }
//        }
//    }
//
//    public synchronized boolean containsPage(PageId pageId) {
//        return pageToTransactionsMap.containsKey(pageId);
//    }
//}
