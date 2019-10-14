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

import com.google.common.collect.Lists;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.gridgain.TestDiscoveryMetricsProvider;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Topology snapshot test.
 */
public class TopologySnapshotTest {
    /**
     * Should create full topology from baseline and cluster nodes.
     */
    @Test
    public void topology() {
        UUID clusterNodeId_1 = UUID.fromString("b-b-b-b-b");
        String consistentNodeId_1 = UUID.fromString("c-c-c-c-c").toString();
        TcpDiscoveryNode clusterNode_1 = new TcpDiscoveryNode(
            clusterNodeId_1,
            Lists.newArrayList("127.0.0.1"),
            Collections.emptyList(),
            8080,
            new TestDiscoveryMetricsProvider(),
            IgniteProductVersion.fromString("1.2.3-0-DEV"),
            consistentNodeId_1
        );
        clusterNode_1.setAttributes(Collections.emptyMap());

        UUID clusterNodeId_2 = UUID.fromString("e-e-e-e-e");
        String consistentNodeId_2 = UUID.fromString("d-d-d-d-d").toString();
        TcpDiscoveryNode clusterNode_2 = new TcpDiscoveryNode(
            clusterNodeId_2,
            Lists.newArrayList("127.0.0.1"),
            Collections.emptyList(),
            8080,
            new TestDiscoveryMetricsProvider(),
            IgniteProductVersion.fromString("1.2.3-0-DEV"),
            consistentNodeId_2
        );
        clusterNode_2.setAttributes(Collections.emptyMap());

        DetachedClusterNode onlineBaselineNode = new DetachedClusterNode(
            consistentNodeId_1,
            Collections.emptyMap()
        );

        String consistentNodeIdOffline = UUID.fromString("a-a-a-a-a").toString();
        DetachedClusterNode oflineBaselineNode = new DetachedClusterNode(
            consistentNodeIdOffline,
            Collections.emptyMap()
        );

        UUID crdId = UUID.fromString("c-c-c-c-c");
        TopologySnapshot top = TopologySnapshot.topology(
            1,
            crdId,
            Lists.newArrayList(clusterNode_1, clusterNode_2),
            Lists.newArrayList(onlineBaselineNode, oflineBaselineNode)
        );

        assertEquals(1, top.getTopologyVersion());
        assertEquals(crdId.toString(), top.getCoordinatorConsistentId());
        assertEquals(3, top.getNodes().size());

        for (Node node : top.getNodes()) {
            if (consistentNodeId_1.equals(node.getConsistentId())) {
                assertTrue(node.isOnline());
                assertTrue(node.isBaselineNode());
                assertFalse(node.isClient());
                assertEquals(clusterNodeId_1, node.getNodeId());
            }

            if (consistentNodeIdOffline.equals(node.getConsistentId())) {
                assertFalse(node.isOnline());
                assertTrue(node.isBaselineNode());
            }

            if (consistentNodeId_2.equals(node.getConsistentId())) {
                assertTrue(node.isOnline());
                assertFalse(node.isBaselineNode());
            }
        }
    }
}
