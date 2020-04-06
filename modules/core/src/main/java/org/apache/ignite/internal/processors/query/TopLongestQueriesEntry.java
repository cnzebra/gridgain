/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.util.typedef.internal.S;

public class TopLongestQueriesEntry {
    private final String schema;
    private final String qry;
    private final boolean loc;
    private final long startTime;
    private final long duration;

    public TopLongestQueriesEntry(String schema, String qry, boolean loc, long startTime, long duration) {
        this.schema = schema;
        this.qry = qry;
        this.loc = loc;
        this.startTime = startTime;
        this.duration = duration;
    }

    public String schema() {
        return schema;
    }

    public String query() {
        return qry;
    }

    public boolean local() {
        return loc;
    }

    public long startTime() {
        return startTime;
    }

    public long duration() {
        return duration;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TopLongestQueriesEntry.class, this);
    }
}
