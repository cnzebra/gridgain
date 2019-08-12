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

package org.apache.ignite.internal.client;

/**
 *  Interface for manage state of grid cluster.
 */
public interface GridClientClusterState {
    /**
     * @param active {@code True} activate, {@code False} deactivate.
     */
    public void active(boolean active) throws GridClientException;

    /**
     * @return {@code Boolean} - Current cluster state. {@code True} active, {@code False} inactive.
     */
    public boolean active() throws GridClientException;

    /**
     * @return {@code True} if the cluster is in read-only mode and {@code false} otherwise.
     * @throws GridClientException If request current cluster read-only mode failed.
     */
    public boolean readOnly() throws GridClientException;

    /**
     * Enable or disable Ignite grid read-only mode.
     *
     * @param readOnly If {@code True} enable read-only mode. If {@code false} disable read-only mode.
     * @throws GridClientException If change of read-only mode is failed.
     */
    public void readOnly(boolean readOnly) throws GridClientException;
}
