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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;

/**
 *
 */
public class RebalanceReassignExchangeTask implements CachePartitionExchangeWorkerTask {
    /** */
    private final GridDhtPartitionExchangeId exchId;

    /** Node ids that should not be used for historical rebalance. */
    private final Set<UUID> nodeIds;

    /**
     * @param exchId Exchange ID.
     */
    public RebalanceReassignExchangeTask(GridDhtPartitionExchangeId exchId) {
        assert exchId != null;

        this.exchId = exchId;
        this.nodeIds = null;
    }

    /**
     * @param exchId Exchange ID.
     * @param nodeIds Node id that has failed historical rebalance.
     */
    public RebalanceReassignExchangeTask(GridDhtPartitionExchangeId exchId, Set<UUID> nodeIds) {
        assert exchId != null;

        this.exchId = exchId;
        this.nodeIds = nodeIds;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Node ids that should not be used for historical rebalance. Returned value can be {@code null}
     */
    public Set<UUID> historicalExclusions() {
        return nodeIds;
    }
}
