package simpledb.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


// 实现四：综合标准库ConcurrentHashMap和LinkedHashMap实现LRU，
// 结合了ConcurrentHashMap的并发性和LinkedHashMap天然的LRU结构。
// ConcurrentHashMap作为对外接口，提供并发读能力。
// LinkedHashMap维持LRU特性和结构。

// 我们使用同步块来确保不会在同一时间修改同一entry
// (内部对写操作的实现不使用同步块也行，但是either 需要外部实现线程安全；
// or 外部不会在同一时间修改同一entry或者其他有相互依赖的操作)
// 在这里，其实可以不使用同步代码块，
// 因为外部实现了对LRU的remove操作的同步，
// 而对于put操作，唯一在同一时间修改同一entry是loadAndCachePage()，
// 但是由于读共享写排他，所以不同事务不可能在同一时间put同一页从而造成不一致。
// HOWEVER, 为了并发语义的正确以及后期的可读、维护、扩展。在这里不必追求极致的性能，仍然使用同步代码块。

// Overall, this implementation provides a basic LRU caching mechanism that is thread-safe and able to handle concurrent access.
// However, it may not be suitable for high-performance applications
// or situations where eviction of items from the cache needs to be fine-tuned.
public class LRUCache<K, V> {

    // Concurrent read operations on a ConcurrentHashMap do not block other read operations,
    // allowing multiple threads to access the map concurrently without interfering with each other.
    // Write operations are also isolated,
    // meaning that they do not block concurrent read operations,
    // allowing multiple threads to read from the map while a single thread is modifying it.
    // Write operations use a lock-free algorithm that minimizes contention and ensures that modifications are performed atomically.
    private volatile ConcurrentHashMap<K, V>    cache;
    private volatile int                        maxSize;
    // maintaining a separate LinkedHashMap, to store the keys in the order in which they were accessed.
    private volatile LinkedHashMap<K, V>        lruMap;

    public LRUCache(int maxSize) {
        this.maxSize    = maxSize + maxSize/10;
        this.cache      = new ConcurrentHashMap<>(maxSize + maxSize/10, 0.75f, 16);
        this.lruMap     = new LinkedHashMap<>(maxSize  + maxSize/10, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > maxSize;
            }
        };
    }

    public V get(K key) {
        return cache.get(key);
    }

    public boolean containsKey(K key) { return cache.containsKey(key);}

    public void put(K key, V value) {
        synchronized (key) {
            V oldValue = cache.put(key, value);
            lruMap.put(key, value);

            // 在这里不可以由LRUCache决定evict哪一页，因为可能evict掉脏页
//            if (oldValue == null && lruMap.size() > maxSize) {
//                synchronized (this) {
//                    if (lruMap.size() > maxSize) {
//                        K eldestKey = lruMap.keySet().iterator().next();
//                        lruMap.remove(eldestKey);
//                        cache.remove(eldestKey);
//                    }
//                }
//            }
        }
    }

    public void remove(K key) {
        synchronized (key) {
            lruMap.remove(key);
            cache.remove(key);
        }
    }

    public int size() {
        return cache.size();
    }

    public Set<K> keySet() { return cache.keySet(); }

    public Collection<V> values() { return cache.values(); }

    public Set<Map.Entry<K, V>> entrySet() {
        return cache.entrySet();
    }

//    public synchronized Iterator<Map.Entry<K, V>> iterator() {
//        return new Iterator<Map.Entry<K, V>>() {
//            private Iterator<Map.Entry<K, V>> cacheIterator = cache.entrySet().iterator();
//
//            @Override
//            public boolean hasNext() {
//                return cacheIterator.hasNext();
//            }
//
//            @Override
//            public Map.Entry<K, V> next() {
//                return cacheIterator.next();
//            }
//
//            @Override
//            public void remove() {
//                cacheIterator.remove();
//            }
//        };
//    }


// This statement means that while ConcurrentHashMap provides thread-safety for individual read and write operations,
// it does not guarantee consistency or correctness across multiple operations performed by multiple threads.
//
// For example, if two threads simultaneously modify different entries in a ConcurrentHashMap,
// the map will ensure that each operation is performed atomically and without interference from the other thread.
// However, if the two threads modify the same entry at the same time,
// or if they perform a series of operations that depend on each other,
// there is a risk of race conditions and data inconsistencies unless proper synchronization is used.
//
// In other words, while ConcurrentHashMap provides a high level of isolation for individual operations,
// it does not provide transactional consistency,
// meaning that concurrent modifications to the map may still lead to inconsistencies and errors unless proper synchronization is maintained.
//
// To ensure consistency and correctness across multiple operations performed by multiple threads,
// it's important to use appropriate synchronization mechanisms, such as locks, to coordinate access to shared resources like ConcurrentHashMap.

}



// 实现三：继承标准库LinkedHashMap实现LRU，在外部借用Collections.synchronizedMap实现线程安全。
//public class LRUCache<K, V> extends LinkedHashMap {
//    private int capacity;
//
//    public LRUCache(int capacity) {
//        super(capacity, 0.75F, true);
//        this.capacity = capacity;
//    }
//
//    public LRUCache() {
//        super();
//        this.capacity = 16;
//    }
//}


// 实现二：借用标准库ConcurrentHashMap实现线程安全，自己手撸实现LRU。
//public class LRUCache<K, V> {
//
//    // LruCache node
//    public class Node {
//        public Node pre;
//        public Node next;
//        public K    key;
//        public V    value;
//
//        public Node(final K key, final V value) {
//            this.key = key;
//            this.value = value;
//        }
//    }
//
//    private final int          maxSize;
//    private final Map<K, Node> nodeMap;
//    private final Node         head;
//    private final Node         tail;
//
//    public LRUCache(int maxSize) {
//        this.maxSize    = maxSize;
//        this.head       = new Node(null, null);
//        this.tail       = new Node(null, null);
//        this.head.next  = tail;
//        this.tail.pre   = head;
//        this.nodeMap    = new ConcurrentHashMap<>();
//
//    }
//
//    public void linkToHead(Node node) {
//        Node next   = this.head.next;
//        node.next   = next;
//        node.pre    = this.head;
//
//        this.head.next  = node;
//        next.pre        = node;
//    }
//
//    public void moveToHead(Node node) {
//        removeNode(node);
//        linkToHead(node);
//    }
//
//    public void removeNode(Node node) {
//        if (node.pre != null && node.next != null) {
//            node.pre.next = node.next;
//            node.next.pre = node.pre;
//        }
//    }
//
//    public Node removeLast() {
//        Node last = this.tail.pre;
//        removeNode(last);
//        return last;
//    }
//
//    public void remove(K key) {
//        if (this.nodeMap.containsKey(key)) {
//            final Node node = this.nodeMap.get(key);
//            removeNode(node);
//            this.nodeMap.remove(key);
//        }
//    }
//
//    public V get(K key) {
//        if (this.nodeMap.containsKey(key)) {
//            Node node = this.nodeMap.get(key);
//            moveToHead(node);
//            return node.value;
//        }
//        return null;
//    }
//
//    public V getOrDefault(K key, V defaultValue) {
//        V v;
//        return (v = get(key)) == null ? defaultValue : v;
//    }
//
//    public V put(K key, V value) {
//        if (this.nodeMap.containsKey(key)) {
//            Node node = this.nodeMap.get(key);
//            node.value = value;
//            moveToHead(node);
//        } else {
//            // We can't remove page here, because we should implement the logic of evict page in BufferPool
//            //            if (this.nodeMap.size() == this.maxSize) {
//            //                Node last = removeLast();
//            //                this.nodeMap.remove(last.key);
//            //                return last.value;
//            //            }
//            Node node = new Node(key, value);
//            this.nodeMap.put(key, node);
//            linkToHead(node);
//        }
//        return null;
//    }
//}


// 实现一：使用标准库HashMap，自己手撸实现LRU和线程安全。
//public class LRUCache<K, V> {
//
//    // LruCache node
//    public class Node {
//        public Node pre;
//        public Node next;
//        public K    key;
//        public V    value;
//
//        public Node(final K key, final V value) {
//            this.key = key;
//            this.value = value;
//        }
//    }
//
//    private final int          maxSize;
//    private final Map<K, Node> nodeMap;
//    private final Node         head;
//    private final Node         tail;
//
//    public LRUCache(int maxSize) {
//        this.maxSize    = maxSize;
//        this.head       = new Node(null, null);
//        this.tail       = new Node(null, null);
//        this.head.next  = tail;
//        this.tail.pre   = head;
//        this.nodeMap    = new HashMap<>();
//
//    }
//
//    public void linkToHead(Node node) {
//        Node next   = this.head.next;
//        node.next   = next;
//        node.pre    = this.head;
//
//        this.head.next  = node;
//        next.pre        = node;
//    }
//
//    public void moveToHead(Node node) {
//        removeNode(node);
//        linkToHead(node);
//    }
//
//    public void removeNode(Node node) {
//        if (node.pre != null && node.next != null) {
//            node.pre.next = node.next;
//            node.next.pre = node.pre;
//        }
//    }
//
//    public Node removeLast() {
//        Node last = this.tail.pre;
//        removeNode(last);
//        return last;
//    }
//
//    public synchronized void remove(K key) {
//        if (this.nodeMap.containsKey(key)) {
//            final Node node = this.nodeMap.get(key);
//            removeNode(node);
//            this.nodeMap.remove(key);
//        }
//    }
//
//    public synchronized V get(K key) {
//        if (this.nodeMap.containsKey(key)) {
//            Node node = this.nodeMap.get(key);
//            moveToHead(node);
//            return node.value;
//        }
//        return null;
//    }
//
//    public synchronized V getOrDefault(K key, V defaultValue) {
//        V v;
//        return (v = get(key)) == null ? defaultValue : v;
//    }
//
//    public synchronized V put(K key, V value) {
//        if (this.nodeMap.containsKey(key)) {
//            Node node = this.nodeMap.get(key);
//            node.value = value;
//            moveToHead(node);
//        } else {
//            // We can't remove page here, because we should implement the logic of evict page in BufferPool
//            //            if (this.nodeMap.size() == this.maxSize) {
//            //                Node last = removeLast();
//            //                this.nodeMap.remove(last.key);
//            //                return last.value;
//            //            }
//            Node node = new Node(key, value);
//            this.nodeMap.put(key, node);
//            linkToHead(node);
//        }
//        return null;
//    }
//}
