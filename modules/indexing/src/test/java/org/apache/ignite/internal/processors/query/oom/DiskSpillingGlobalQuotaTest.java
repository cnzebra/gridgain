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
package org.apache.ignite.internal.processors.query.oom;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

/**
 * Global quota test.
 */
@WithSystemProperty(key = "IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE", value = "16384")
public class DiskSpillingGlobalQuotaTest extends DiskSpillingAbstractTest {
    /** */
    @Test
    public void testGlobalQuotaCausesDiskSpilling() throws IOException {
        String qry = "SELECT DISTINCT *  " +
            "FROM person p, department d " +
            " WHERE p.depId = d.id";

        Path workDir = getWorkDir();

        WatchService watchSvc = FileSystems.getDefault().newWatchService();

        WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

        grid(0).cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery(qry))
            .getAll();

        List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

        // Check files have been created but deleted later.
        assertFalse(dirEvts.isEmpty());

        assertWorkDirClean();
    }
}
