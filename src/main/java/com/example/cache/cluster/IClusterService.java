package com.example.cache.cluster;

import java.util.Set;

/**
 * This interface manages cluster membership and key ownership.
 * This is a core component for routing requests in distributed cache.
 */
public interface IClusterService<K> {

    /**
     * Retrieves the unique identifier of local node instance
     * @return The local node ID
     */
    String getLocalNodeId();

    /**
     * Define the owner node for a given using the consistent hashing algorithm.
     * This allows client to efficiently route the requests to the correct node.
     * @param key The key to look up
     * @return The ID of the node storing this key
     */
    String findOwnerNode(K key);

    /**
     * Get a read only set of all active node IDs in the cluster.
     * @return A set of active node IDs
     */
    Set<String> getAllNodeIds();

    /**
     * Adds a new node to cluster membership ring.
     * @param nodeId The ID of the new node.
     */
    void addNode(String nodeId);

    /**
     * Removes an existing node from the cluster membership ring.
     * @param nodeId The ID of the node to remove.
     */
    void removeNode(String nodeId);

    /**
     * Checks if the consistent hash ring has been initialized and have at least one node.
     * @return True if the cluster is ready to handle distributed requests.
     */
    boolean isClusterReady();
}
