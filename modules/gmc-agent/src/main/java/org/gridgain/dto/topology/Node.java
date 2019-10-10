/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.dto.topology;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Data transfer object with node info.
 */
public class Node {
    /** Set of attributes required by GMC. */
    private static final Set<String> GMC_ATTRS = Stream.of(
        ATTR_IPS,
        ATTR_MACS,
        ATTR_JVM_PID,
        ATTR_BUILD_VER
    ).collect(toSet());

    /** */
    private UUID nid;

    /** */
    private String consistentId;

    /** */
    private boolean client;

    /** */
    private Map<String, Object> attrs;

    /**
     * Default constructor for serialization.
     */
    public Node() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param nid Node ID.
     * @param consistentId Consistent ID.
     * @param client Client flag.
     * @param attrs Node attributes.
     */
    public Node(UUID nid, Object consistentId, boolean client, Map<String, Object> attrs) {
        this.nid = nid;
        this.consistentId = String.valueOf(consistentId);
        this.client = client;
        this.attrs = attrs
            .entrySet()
            .stream()
            .filter(e -> GMC_ATTRS.contains(e.getKey()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Constructor from cluster node.
     *
     * @param node Cluster node.
     */
    public Node(ClusterNode node) {
        this(node.id(), node.consistentId(), node.isClient(), node.attributes());
    }

    /**
     * Constructor from baseline node.
     *
     * @param node Baseline node.
     */
    public Node(BaselineNode node) {
        this(null, node.consistentId(), false, node.attributes());
    }

    /**
     * @return Node ID.
     */
    public UUID getNodeId() {
        return nid;
    }

    /**
     * @param nid Node ID.
     */
    public void setNodeId(UUID nid) {
        this.nid = nid;
    }

    /**
     * @return Node consistent ID.
     */
    public String getConsistentId() {
        return consistentId;
    }

    /**
     * @param consistentId Node consistent ID.
     */
    public void setConsistentId(String consistentId) {
        this.consistentId = consistentId;
    }

    /**
     * @return {@code true} for client node.
     */
    public boolean isClient() {
        return client;
    }

    /**
     * @param client {@code true} for client node.
     */
    public void setClient(boolean client) {
        this.client = client;
    }

    /**
     * @return Node attributes.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }

    /**
     * @param attrs Node attributes.
     */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Node.class, this);
    }
}