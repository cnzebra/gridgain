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
package org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask;

import java.io.Serializable;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.S;

public abstract class AbstractPendingNodeTask implements Serializable {
    /** */
    protected StoredCacheData changedCacheData;

    /** */
    public AbstractPendingNodeTask() {
        /* No op. */
    }

    /** */
    public StoredCacheData changedCacheData() {
        return changedCacheData;
    }

    /** */
    public abstract String shortName();

    /** */
    public abstract void execute(GridKernalContext ctx);

    /** */
    public abstract void onCreate(GridKernalContext ctx);

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(AbstractPendingNodeTask.class, this);
    }
}
