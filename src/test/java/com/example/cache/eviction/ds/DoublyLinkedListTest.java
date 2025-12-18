package com.example.cache.eviction.ds;

import com.example.cache.eviction.ds.domain.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DoublyLinkedListTest {

    private DoublyLinkedList<String, Void> doublyLinkedList;

    @BeforeEach
    public void setUp() {
        doublyLinkedList = new DoublyLinkedList<>();
    }

    @Test
    public void testInsertFirst() {
        String[] items = new String[]{"test_0", "test_1"};
        doublyLinkedList.insertFirst(items[0], null);
        doublyLinkedList.insertFirst(items[1], null);

        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(2, doublyLinkedList.size());
        Assertions.assertEquals(items[1], doublyLinkedList.getFirst().getData());
        Assertions.assertEquals(items[0], doublyLinkedList.getLast().getData());
    }

    @Test
    public void testInsertLast() {
        String[] items = new String[]{"test_0", "test_1"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);

        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(2, doublyLinkedList.size());
        Assertions.assertEquals(items[1], doublyLinkedList.getLast().getData());
        Assertions.assertEquals(items[0], doublyLinkedList.getFirst().getData());
    }

    @Test
    public void testInsertBefore() {
        String[] items = new String[]{"test_0", "test_1", "test_2", "test_3"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);

        doublyLinkedList.insertBefore(doublyLinkedList.getLast(), items[2], null);
        doublyLinkedList.insertBefore(doublyLinkedList.getFirst(), items[3], null);

        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(items.length, doublyLinkedList.size());
        Assertions.assertEquals(items[3], doublyLinkedList.getFirst().getData());
        Assertions.assertEquals(items[0], doublyLinkedList.getFirst().getNext().getData());
        Assertions.assertEquals(items[2], doublyLinkedList.getFirst().getNext().getNext().getData());
        Assertions.assertEquals(items[1], doublyLinkedList.getFirst().getNext().getNext().getNext().getData());
        Assertions.assertEquals(items[1], doublyLinkedList.getLast().getData());
    }

    @Test
    public void testInsertAfter() {
        String[] items = new String[]{"test_0", "test_1", "test_2", "test_3"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);

        doublyLinkedList.insertAfter(doublyLinkedList.getLast(), items[2], null);
        doublyLinkedList.insertAfter(doublyLinkedList.getFirst(), items[3], null);

        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(items.length, doublyLinkedList.size());
        Assertions.assertEquals(items[0], doublyLinkedList.getFirst().getData());
        Assertions.assertEquals(items[3], doublyLinkedList.getFirst().getNext().getData());
        Assertions.assertEquals(items[1], doublyLinkedList.getFirst().getNext().getNext().getData());
        Assertions.assertEquals(items[2], doublyLinkedList.getFirst().getNext().getNext().getNext().getData());
        Assertions.assertEquals(items[2], doublyLinkedList.getLast().getData());
    }

    @Test
    public void testDeleteOnlyElement() {
        String item = "test";
        doublyLinkedList.insertLast(item, null);
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(1, doublyLinkedList.size());

        doublyLinkedList.deleteNode(doublyLinkedList.getLast());
        Assertions.assertTrue(doublyLinkedList.isEmpty());
        Assertions.assertEquals(0, doublyLinkedList.size());
    }

    @Test
    public void testDeleteFirstElement() {
        String[] items = new String[]{"test_0", "test_1", "test_2", "test_3"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);
        doublyLinkedList.insertLast(items[2], null);
        doublyLinkedList.insertLast(items[3], null);
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(items.length, doublyLinkedList.size());

        doublyLinkedList.deleteNode(doublyLinkedList.getFirst());
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(3, doublyLinkedList.size());
        Assertions.assertEquals(items[1], doublyLinkedList.getFirst().getData());
    }

    @Test
    public void testDeleteLastElement() {
        String[] items = new String[]{"test_0", "test_1", "test_2", "test_3"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);
        doublyLinkedList.insertLast(items[2], null);
        doublyLinkedList.insertLast(items[3], null);
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(items.length, doublyLinkedList.size());

        doublyLinkedList.deleteNode(doublyLinkedList.getLast());
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(3, doublyLinkedList.size());
        Assertions.assertEquals(items[2], doublyLinkedList.getLast().getData());
    }

    @Test
    public void testDeleteInLineElement() {
        String[] items = new String[]{"test_0", "test_1", "test_2", "test_3"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);
        doublyLinkedList.insertLast(items[2], null);
        doublyLinkedList.insertLast(items[3], null);
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(items.length, doublyLinkedList.size());

        doublyLinkedList.deleteNode(doublyLinkedList.getLast().getPrev());
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(3, doublyLinkedList.size());
        Assertions.assertEquals(items[1], doublyLinkedList.getLast().getPrev().getData());
    }

    @Test
    public void testDeleteFirstWithValidList() {
        String[] items = new String[] {"test_0", "test_1", "test_2", "test_3"};
        doublyLinkedList.insertLast(items[0], null);
        doublyLinkedList.insertLast(items[1], null);
        doublyLinkedList.insertLast(items[2], null);
        doublyLinkedList.insertLast(items[3], null);
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(items.length, doublyLinkedList.size());

        {
            Node<String, Void> firstElement = doublyLinkedList.deleteFirst();
            Assertions.assertEquals(items[0], firstElement.getData());
        }
        {
            Node<String, Void> firstElement = doublyLinkedList.deleteFirst();
            Assertions.assertEquals(items[1], firstElement.getData());
        }
        {
            Node<String, Void> firstElement = doublyLinkedList.deleteFirst();
            Assertions.assertEquals(items[2], firstElement.getData());
        }
    }

    @Test
    public void testDeleteFirstWithEmptyList() {
        Assertions.assertTrue(doublyLinkedList.isEmpty());
        Assertions.assertThrows(IllegalStateException.class, doublyLinkedList::deleteFirst);
    }

    @Test
    public void testDeleteFirstWithSingleElementInList() {
        String item = "test";
        doublyLinkedList.insertLast(item, null);
        Assertions.assertFalse(doublyLinkedList.isEmpty());
        Assertions.assertEquals(1, doublyLinkedList.size());

        {
            Node<String, Void> firstElement = doublyLinkedList.deleteFirst();
            Assertions.assertEquals(item, firstElement.getData());
        }
        {
            Assertions.assertTrue(doublyLinkedList.isEmpty());
            Assertions.assertThrows(IllegalStateException.class, doublyLinkedList::deleteFirst);
        }
    }
}
