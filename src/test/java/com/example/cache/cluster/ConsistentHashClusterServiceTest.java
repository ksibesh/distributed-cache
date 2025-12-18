package com.example.cache.cluster;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentHashClusterServiceTest {

    private final String LOCAL_NODE_ID = "local-node-1";
    private final int VIRTUAL_NODE_COUNT = 2;

    // intentionally avoided @BeforeEach, as we are changing the state of clusterService in each test.
    public ConsistentHashClusterService<String> setup() {
        return new ConsistentHashClusterService<>(LOCAL_NODE_ID, VIRTUAL_NODE_COUNT);
    }

    @Test
    public void testGetLocalNodeId() {
        ConsistentHashClusterService<String> clusterService = setup();
        assertEquals(LOCAL_NODE_ID, clusterService.getLocalNodeId());
    }

    @Test
    public void testGetAllNodeIdWithNoNode() {
        ConsistentHashClusterService<String> clusterService = setup();

        Set<String> allNodes = clusterService.getAllNodeIds();
        assertNotNull(allNodes);
        assertEquals(0, allNodes.size());
    }

    @Test
    public void testGetAllNodesWithMultipleNodes() {
        ConsistentHashClusterService<String> clusterService = setup();
        clusterService.addNode("node-1");
        clusterService.addNode("node-2");
        clusterService.addNode("node-3");

        Set<String> allNodes = clusterService.getAllNodeIds();
        assertNotNull(allNodes);
        assertEquals(3, allNodes.size());
    }

    @Test
    public void testAddNode() {
        ConsistentHashClusterService<String> clusterService = setup();
        {
            Set<String> allNodes = clusterService.getAllNodeIds();
            assertNotNull(allNodes);
            assertEquals(0, allNodes.size());
        }
        {
            clusterService.addNode("node-1");
            clusterService.addNode("node-2");

            Set<String> allNodes = clusterService.getAllNodeIds();
            assertEquals(2, allNodes.size());
        }
    }

    @Test
    public void testRemoveNode() {
        String[] nodeIds = new String[]{"node-1", "node-2", "node-3"};

        ConsistentHashClusterService<String> clusterService = setup();
        clusterService.addNode(nodeIds[0]);
        clusterService.addNode(nodeIds[1]);
        clusterService.addNode(nodeIds[2]);
        {
            Set<String> allNodes = clusterService.getAllNodeIds();
            assertEquals(nodeIds.length, allNodes.size());
        }
        {
            clusterService.removeNode(nodeIds[1]);

            Set<String> allNodes = clusterService.getAllNodeIds();
            assertEquals(nodeIds.length - 1, allNodes.size());
        }
    }

    @Test
    public void testClusterReadyForEmptyRing() {
        ConsistentHashClusterService<String> clusterService = setup();

        assertFalse(clusterService.isClusterReady());
    }

    @Test
    public void testClusterReadyForRingWithItems() {
        String[] nodeIds = new String[]{"node-1", "node-2", "node-3"};

        ConsistentHashClusterService<String> clusterService = setup();
        clusterService.addNode(nodeIds[0]);
        clusterService.addNode(nodeIds[1]);
        clusterService.addNode(nodeIds[2]);

        assertTrue(clusterService.isClusterReady());
    }

    @Test
    public void testFindOwnerNodeAndDistribution() {
        ConsistentHashClusterService<String> clusterService = setup();
        String nodeA = "node-A";
        String nodeB = "node-B";
        String nodeC = "node-C";

        clusterService.addNode(nodeA);
        clusterService.addNode(nodeB);
        clusterService.addNode(nodeC);

        String[] testKeys = new String[]{"key1", "user:5000", "session:abc", "data:xyz"};
        Map<String, String> initialOwners = new HashMap<>();
        Set<String> uniqueOwners = new HashSet<>();

        for (String key : testKeys) {
            String initialOwner = clusterService.findOwnerNode(key);
            assertTrue(clusterService.getAllNodeIds().contains(initialOwner));

            initialOwners.put(key, initialOwner);
            uniqueOwners.add(initialOwner);

            // check determinism, hash should not change on different calls
            for (int i = 0; i < 10; i++) {
                assertEquals(initialOwner, clusterService.findOwnerNode(key));
            }
        }
        // Check basic distribution; keys should be distributed across at-least 2 nodes
        assertTrue(uniqueOwners.size() >= 2);

        String nodeD = "node-D";
        clusterService.addNode(nodeD);

        int keysMoved = 0;
        for (String key : testKeys) {
            String originalOwner = initialOwners.get(key);
            String newOwner = clusterService.findOwnerNode(key);
            assertTrue(clusterService.getAllNodeIds().contains(newOwner));

            if (!newOwner.equals(originalOwner)) {
                keysMoved++;
            }
        }
        // with consistent hashing only small percentage of keys should move
        assertTrue(keysMoved <= 2);
        assertTrue(clusterService.getAllNodeIds().contains(nodeD));
    }
}
