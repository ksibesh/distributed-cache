package com.example.cache.eviction.ds.domain;

import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
public class Node<Item, Metadata> {
    private Node<Item, Metadata> prev;
    private Node<Item, Metadata> next;
    private Item data;
    private Metadata metadata;

    public Node(Item data, Metadata metadata) {
        this.next = this.prev = null;
        this.data = data;
        this.metadata = metadata;
    }
}
