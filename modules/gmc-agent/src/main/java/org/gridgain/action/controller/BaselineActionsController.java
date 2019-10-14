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

package org.gridgain.action.controller;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.gridgain.action.annotation.ActionController;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.gridgain.utils.AgentUtils.authorizeIfNeeded;

/**
 * Baseline actions controller.
 */
@ActionController("BaselineActions")
public class BaselineActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public BaselineActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param isAutoAdjustEnabled Is auto adjust enabled.
     * @return Completeble feature.
     */
    public void updateAutoAdjustEnabled(boolean isAutoAdjustEnabled) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().baselineAutoAdjustEnabled(isAutoAdjustEnabled);
    }

    /**
     * @param awaitingTime Awaiting time in ms.
     * @return Completeble feature.
     */
    public void updateAutoAdjustAwaitingTime(long awaitingTime) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().baselineAutoAdjustTimeout(awaitingTime);
    }

    /**
     * @param ids Node ids.
     * @return Completeble feature.
     */
    public void setBaselineTopology(Collection<String> ids) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        Set<UUID> uuids = ids.stream().map(UUID::fromString).collect(Collectors.toSet());

        Collection<ClusterNode> nodes = ctx.grid().cluster().forNodeIds(uuids).forServers().nodes();
        ctx.grid().cluster().setBaselineTopology(nodes);
    }
}
