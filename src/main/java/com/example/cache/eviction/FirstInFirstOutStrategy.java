package com.example.cache.eviction;

import com.example.cache.eviction.ds.DoublyLinkedList;
import com.example.cache.eviction.ds.domain.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class FirstInFirstOutStrategy<K> implements IEvictionStrategy<K> {
    private final DoublyLinkedList<K, Void> queue;
    private final Map<K, Node<K, Void>> elementMap;

    public FirstInFirstOutStrategy() {
        this.queue = new DoublyLinkedList<>();
        this.elementMap = new HashMap<>();
    }

    /**
     * Overall complexity of this method id O(1), as we are either performing get on Map or inserting at the end of
     * Doubly Linked List, both of these operations have O(1) complexity.
     *
     * @param key - key of the cache that need to be maintained.
     */
    @Override
    public void onPut(K key) {
        Node<K, Void> existingNode = elementMap.get(key);
        if (existingNode != null) {
            // key already exist, generally a cache replace the value of key if another put is done on existing element;
            // we can also follow the similar approach, if item already exist we can remove the current existence and
            // add the new one at the end of the queue.
            queue.deleteNode(existingNode); // deleteNode function handles all the cases of single, first, last, and in-between node
            queue.insertLast(existingNode.getData(), null);
            elementMap.put(key, queue.getLast());   // updating the element map with new node
        } else {
            // key doesn't exist, that mean it doesn't exist in the cache; it the simple situation we just need to add
            // the key at the back of the queue and add the entry in the elementMap for future look up.
            queue.insertLast(key, null);
            elementMap.put(key, queue.getLast());
        }
    }

    @Override
    public void onAccess(K key) {
        // There should be no-op, as from the concept of FIFO the element that was inserted fist should be evicted first
        // it doesn't matter how many times the object was queried.
    }

    @Override
    public void onRemove(K key) {
        // Here we will use the elementMap to identify the position of the key in the Doubly Linked List; this method
        // is the reason we didn't used Java version of Queue to maintain the keys, as with traditional queue on deletion
        // we have to traverse the whole DS to find the object.
        Node<K, Void> queriedNode = elementMap.get(key);
        if (queriedNode != null) {
            queue.deleteNode(queriedNode);  // deleteNode function handles all the cases of single, first, last, and in-between node
            elementMap.remove(key);
        }
        // No need to perform any operation if key is not present in elementMap, there is nothing to delete in that case.
    }

    @Override
    public Optional<K> evict() {
        return Optional.ofNullable(queue.getFirst()).map(Node::getData);
    }
}
