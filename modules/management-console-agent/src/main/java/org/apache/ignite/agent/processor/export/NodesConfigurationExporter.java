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

package org.apache.ignite.agent.processor.export;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.agent.dto.IgniteConfigurationWrapper;
import org.apache.ignite.agent.dto.NodeConfiguration;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

import static org.apache.ignite.agent.ManagementConsoleAgent.TOPIC_MANAGEMENT_CONSOLE;
import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.jsonMapper;
import static org.apache.ignite.agent.utils.FakeUtils.exportFakeNodeConfigs;

/**
 * Node configuration exporter.
 */
public class NodesConfigurationExporter extends GridProcessorAdapter {
    /** Mapper. */
    private final ObjectMapper mapper = jsonMapper();

    /**
     * @param ctx Context.
     */
    public NodesConfigurationExporter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Send node configuration to coordinator.
     */
    public void export() {
        try {
            IgniteEx ignite = ctx.grid();

            IgniteClusterEx cluster = ignite.cluster();

            String consistentId = cluster.localNode().consistentId().toString();

            IgniteConfigurationWrapper cfgWrapper = new IgniteConfigurationWrapper(ctx.config());

            String json = mapper.writeValueAsString(cfgWrapper);

            NodeConfiguration nodeCfg = new NodeConfiguration(consistentId, json);

            ClusterGroup oldestNode = cluster.forOldest();

            ignite.message(oldestNode).send(TOPIC_MANAGEMENT_CONSOLE, nodeCfg);

            // GG-28545 Fake node configurations.
            exportFakeNodeConfigs(ignite, cfgWrapper, mapper, oldestNode);
        }
        catch (JsonProcessingException e) {
            log.error("Failed to serialize the IgniteConfiguration to JSON", e);
        }
    }
}
