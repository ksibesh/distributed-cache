package com.example.cache.eviction;

import com.example.cache.eviction.domain.LeastFrequentlyUsedMetadata;
import com.example.cache.eviction.ds.DoublyLinkedList;
import com.example.cache.eviction.ds.domain.Node;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class LeastFrequentlyUsedStrategy<K> implements IEvictionStrategy<K> {

    private final Map<Integer, Node<DoublyLinkedList<K, LeastFrequentlyUsedMetadata>, LeastFrequentlyUsedMetadata>> freqMap;
    private final Map<K, Node<K, LeastFrequentlyUsedMetadata>> elementMap;
    private final DoublyLinkedList<DoublyLinkedList<K, LeastFrequentlyUsedMetadata>, LeastFrequentlyUsedMetadata> outerList;

    public LeastFrequentlyUsedStrategy() {
        this.freqMap = new HashMap<>();
        this.elementMap = new HashMap<>();
        this.outerList = new DoublyLinkedList<>();
    }

    private void coreInsertionAndAccessLogic(K key) {
        Node<K, LeastFrequentlyUsedMetadata> node = elementMap.get(key);

        if (node == null) {
            // insertion flow; node doesn't exist; no need to delete
            LeastFrequentlyUsedMetadata metadata = LeastFrequentlyUsedMetadata.builder().build();
            if (outerList.isEmpty()) {
                // outerList is empty; this is the initial state; simply add the entry in the outerList, elementMap, and freqMap
                outerList.insertLast(new DoublyLinkedList<>(), metadata);
                freqMap.put(metadata.getFrequencyValue(), outerList.getLast());

                outerList.getLast().getData().insertLast(key, metadata);
                elementMap.put(key, outerList.getLast().getData().getLast());
            } else {
                // this is tricky; now we have to check if the first element in the outerList is with freq 1 or not;
                // if not then we have to insert a new DLL in the first position
                if (outerList.getFirst().getMetadata().getFrequencyValue() > 1) {
                    outerList.insertFirst(new DoublyLinkedList<>(), metadata);
                    freqMap.put(metadata.getFrequencyValue(), outerList.getFirst());

                    outerList.getFirst().getData().insertLast(key, metadata);
                    elementMap.put(key, outerList.getFirst().getData().getLast());
                } else {
                    // this mean the first node in the outerList is with freq 1
                    freqMap.get(metadata.getFrequencyValue()).getData().insertLast(key, metadata);
                    elementMap.put(key, freqMap.get(metadata.getFrequencyValue()).getData().getLast());
                    // no need to insert in freqMap in this case, coz if freq 1 node exist in outerList,
                    // that means it exist in freqMap as well
                }
            }
        } else {
            // access flow; node already exist; we have to delete node where it already exists
            int curFreq = node.getMetadata().getFrequencyValue();
            // there should be an entry in the freqMap for curFreq, coz if it doesn't then it means that system is
            // not in correct state
            Node<DoublyLinkedList<K, LeastFrequentlyUsedMetadata>, LeastFrequentlyUsedMetadata> outerNode = freqMap.get(curFreq);
            if (outerNode == outerList.getLast()) {
                // this means that the current key holds the highest frequency;
                // insert a DLL at the last after increasing frequency by 1
                LeastFrequentlyUsedMetadata metadata = LeastFrequentlyUsedMetadata.builder()
                        .frequency(new AtomicInteger(curFreq + 1)).build();
                outerList.insertLast(new DoublyLinkedList<>(), metadata);
                freqMap.put(metadata.getFrequencyValue(), outerList.getLast());

                outerList.getLast().getData().insertLast(key, metadata);
                elementMap.put(key, outerList.getLast().getData().getLast());
            } else {
                // the outerNode is not the last node; that means the current frequency in the highest;
                // we have to check if +1 higher frequency DLL exist; if it doesn't then we have to insert
                // one DLL after the outerNode
                if (outerNode.getNext().getMetadata().getFrequencyValue() > (curFreq + 1)) {
                    LeastFrequentlyUsedMetadata metadata = LeastFrequentlyUsedMetadata.builder()
                            .frequency(new AtomicInteger(curFreq + 1)).build();
                    outerList.insertAfter(outerNode, new DoublyLinkedList<>(), metadata);
                    freqMap.put(metadata.getFrequencyValue(), outerNode.getNext());

                    outerNode.getNext().getData().insertLast(key, metadata);
                    elementMap.put(key, outerNode.getNext().getData().getLast());
                } else {
                    // we have reached this logic; this means the next node frequency is (curFreq + 1);
                    // in this case we have to add the element at last position of the next node;
                    // for safer side we will fetch the (curFreq + 1) from freqMap;
                    // if system is inconsistent then this will fail and become easy to debug
                    Node<DoublyLinkedList<K, LeastFrequentlyUsedMetadata>, LeastFrequentlyUsedMetadata> nextNode =
                            freqMap.get(curFreq + 1);
                    nextNode.getData().insertLast(key, nextNode.getMetadata());
                    elementMap.put(key, nextNode.getData().getLast());
                }
            }
            // Node removal logic
            freqMap.get(curFreq).getData().deleteNode(node);
            if (freqMap.get(curFreq).getData().isEmpty()) {
                outerList.deleteNode(freqMap.get(curFreq));
            }
        }
    }

    @Override
    public void onPut(K key) {
        coreInsertionAndAccessLogic(key);
        log.debug("[Eviction.Strategy.LFU.PUT] [key={}]", key);
    }

    @Override
    public void onAccess(K key) {
        coreInsertionAndAccessLogic(key);
        log.debug("[Eviction.Strategy.LFU.ACCESS] [key={}]", key);
    }

    @Override
    public void onRemove(K key) {
        Node<K, LeastFrequentlyUsedMetadata> node = elementMap.get(key);
        int curFreq = node.getMetadata().getFrequencyValue();
        freqMap.get(curFreq).getData().deleteNode(node);
        if (freqMap.get(curFreq).getData().isEmpty()) {
            outerList.deleteNode(freqMap.get(curFreq));
        }
        elementMap.remove(key);
        log.debug("[Eviction.Strategy.LFU.REMOVE] [key={}]", key);
    }

    @Override
    public Optional<K> evict() {
        if (outerList.isEmpty()) {
            log.debug("[Eviction.Strategy.LRU.EVICT] [<empty>]");
            return Optional.empty();
        }
        Optional<K> evictionEntry = Optional.of(outerList.getFirst().getData().getFirst().getData());
        log.debug("[Eviction.Strategy.LFU.EVICT]");
        return evictionEntry;
    }

}
