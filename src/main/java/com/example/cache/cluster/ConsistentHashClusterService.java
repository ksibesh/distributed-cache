package com.example.cache.cluster;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements IClusterService using Consistent Hashing to manage key distribution.
 * This approach ensure minimal key movement when nodes are added or removed.
 */
@Slf4j
public class ConsistentHashClusterService implements IClusterService {

    private final String localNodeId;
    private final int numberOfVirtualNode;

    // TreeMap represent the hash ring: Key=Hash Value, Value=Node ID
    private final TreeMap<Long, String> hashRing = new TreeMap<>();

    // Set of active physical node IDs
    private final Set<String> activeNodes = ConcurrentHashMap.newKeySet();

    // Map for holding the mapping of node ID to node address
    private final Map<String, String> nodeAddressMap = new ConcurrentHashMap<>();

    // Cache the hash function for performance
    private final ThreadLocal<MessageDigest> md5 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    });

    public ConsistentHashClusterService(String localNodeId, int numberOfVirtualNode) {
        this.localNodeId = localNodeId;
        this.numberOfVirtualNode = numberOfVirtualNode;
        log.info("[ClusterService.ConsistentHashClusterService.Initialized] [localNodeId={}] [numberOfVirtualNode={}]",
                localNodeId, numberOfVirtualNode);
    }

    @Override
    public String getLocalNodeId() {
        return localNodeId;
    }

    @Override
    public String findOwnerNode(String key) {
        if (hashRing.isEmpty()) {
            // Fallback: If no node, assume local node
            return localNodeId;
        }

        long keyHash = hash(key);
        // Find the first node on the ring whose hash is greater than or equal to key's hash
        Long nodeHash = hashRing.ceilingKey(keyHash);
        // If no node is found (i.e. we are at the end of the ring), wrap around the first node
        if (nodeHash == null) {
            nodeHash = hashRing.firstKey();
        }
        return hashRing.get(nodeHash);
    }

    @Override
    public Set<String> getAllNodeIds() {
        return Collections.unmodifiableSet(activeNodes);
    }

    @Override
    public String getAddressForNodeId(String nodeId) {
        String address = nodeAddressMap.get(nodeId);
        if (address == null || address.isEmpty()) {
            log.warn("Address for node={} is empty", nodeId);
        }
        return address;
    }

    @Override
    public void addNode(String nodeId, String nodeAddress) {
        if (activeNodes.add(nodeId)) {
            for (int i = 0; i < numberOfVirtualNode; i++) {
                long hash = hash(nodeId + "-" + i);
                hashRing.put(hash, nodeId);
            }
            nodeAddressMap.put(nodeId, nodeAddress);
            log.info("[ClusterService.ConsistentHashClusterService.AddNode] [Node ID={}] [Node Address={}] " +
                            "[Number of Virtual Nodes={}] [Total Nodes={}]", nodeId, nodeAddress, numberOfVirtualNode,
                    activeNodes.size());
        }
    }

    @Override
    public void addNode(String nodeId) {
        addNode(nodeId, nodeId);
    }

    @Override
    public void removeNode(String nodeId) {
        if (activeNodes.remove(nodeId)) {
            for (int i = 0; i < numberOfVirtualNode; i++) {
                long hash = hash(nodeId + "-" + i);
                hashRing.remove(hash);
            }
            log.warn("[ClusterService.ConsistentHashClusterService.RemoveNode] [NodeId={}] [Number of Virtual Nodes={}] [Total Nodes={}]",
                    nodeId, numberOfVirtualNode, activeNodes.size());
        }
    }

    @Override
    public boolean isClusterReady() {
        return !hashRing.isEmpty();
    }

    /**
     * Generates a 64-bit hash from the input string using MD5
     *
     * @param value The string to hash
     * @return The 64-bit hash value
     */
    private long hash(String value) {
        MessageDigest digest = md5.get();
        digest.reset();
        byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));

        // Use the first 8 bytes of the MD5 hash to create a long (64-bit hash)
        long hash = 0;
        for (int i = 0; i < 8; i++) {
            hash = hash << 8 | (bytes[i] & 0xFF);
        }
        return hash;
    }
}
