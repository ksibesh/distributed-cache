package com.example.cache.eviction.ds;

import com.example.cache.eviction.ds.domain.Node;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

public class DoublyLinkedList<Item, Metadata> {
    @Getter
    private Node<Item, Metadata> first;
    @Getter
    private Node<Item, Metadata> last;
    private final AtomicInteger size = new AtomicInteger(0);

    public void insertFirst(Item data, Metadata metadata) {
        if (first == null) {    // this means empty list
            first = last = new Node<>(data, metadata);
        } else {    // some element exist in the list, insert at last
            first.setPrev(new Node<>(null, first, data, metadata));
            first = first.getPrev();
        }
        size.incrementAndGet();
    }

    public void insertLast(Item data, Metadata metadata) {
        if (first == null) {    // this means empty list
            first = last = new Node<>(data, metadata);
        } else {    // some element exist in the list, insert at last
            last.setNext(new Node<>(last, null, data, metadata));
            last = last.getNext();
        }
        size.incrementAndGet();
    }

    public void insertBefore(Node<Item, Metadata> node, Item data, Metadata metadata) {
        if (node.getPrev() == null) {
            first = new Node<>(null, node, data, metadata);
            node.setPrev(first);
        } else {
            node.getPrev().setNext(new Node<>(node.getPrev(), node, data, metadata));
            node.setPrev(node.getPrev().getNext());
        }
        size.incrementAndGet();
    }

    public void insertAfter(Node<Item, Metadata> node, Item data, Metadata metadata) {
        if (node.getNext() == null) {
            last = new Node<>(node, null, data, metadata);
            node.setNext(last);
        } else {
            node.getNext().setPrev(new Node<>(node, node.getNext(), data, metadata));
            node.setNext(node.getNext().getPrev());
        }
        size.incrementAndGet();
    }

    public void deleteNode(Node<Item, Metadata> node) {
        if (null == node) {
            throw new IllegalArgumentException("'node' cannot be null.");
        }

        if (node.getPrev() == null && node.getNext() == null) {    // only node
            first = last = null;
        } else if (node.getPrev() == null) { // first node
            first = node.getNext();
            node.getNext().setPrev(null);
        } else if (node.getNext() == null) { // last node
            last = node.getPrev();
            node.getPrev().setNext(null);
        } else {    // node is present somewhere in between the DLL
            node.getPrev().setNext(node.getNext());
            node.getNext().setPrev(node.getPrev());
        }
        size.decrementAndGet();
    }

    public Node<Item, Metadata> deleteFirst() {
        if (size.get() == 0) {
            throw new IllegalStateException("List is empty.");
        }

        // no need to check null for first pointer, it is already validated with size, if it comes as null then the code
        // should fail to identify the issue.
        Node<Item, Metadata> deletedNode = first;
        first = first.getNext();
        if (first != null) {
            // this check handles the case when there is just one element in the list, in that case the next of first
            // will be null, and when we try to set the previous of first then it will throw null pointer exception.
            first.setPrev(null);
        }
        size.decrementAndGet();
        deletedNode.setNext(null);
        return deletedNode;
    }

    public int size() {
        return size.get();
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

}
