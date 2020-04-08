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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.h2.DistributedSqlConfiguration;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

/** */
public class SqlTopLongestQueriesTrackerIntegrationTest extends GridCommonAbstractTest {
    /** */
    private TopLongestQueriesTracker tracker;

    /** */
    private DistributedSqlConfiguration distCfg;

    /** */
    private final AtomicBoolean stopped = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        cleanPersistenceDir();

        startGrid(0);
    }

    /** */
    @Before
    public void init() {
        tracker = ((IgniteH2Indexing)grid(0).context().query().getIndexing())
            .runningQueryManager().topLongestQueriesTracker();

        distCfg = ((IgniteH2Indexing)grid(0).context().query().getIndexing())
            .distributedConfiguration();

        tracker.closeSnapshot();
        tracker.closeSnapshot();

        stopped.set(false);
    }

    /** */
    @After
    public void stop() {
        stopped.set(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                defaultCacheConfiguration().setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
            );
    }

    /**
     * Ensure tracker reflects to the minDuration changes:
     *    1) set minDuration to the low value
     *    2) run several queries with different duration
     *    3) verify that top chart contains only queries with duration >= the setted one
     *    4) set new minDuration that bigger then previuos one
     *    5) verify new result
     */
    @Test
    public void testTrackerRespectsMinDurationChange() throws IgniteCheckedException {
        final long windowSize = 500L;

        distCfg.topLongestQueryWindowSize(windowSize).get();

        AtomicInteger threadCnt = new AtomicInteger();

        IgniteInternalFuture<?> runners = GridTestUtils.runMultiThreadedAsync(() -> {
            int grpNo = threadCnt.incrementAndGet() % 3;

            while (!stopped.get()) {
                // 3 groups with delay 50..90, 100..140, 150..190
                long delay = grpNo * 50 + ThreadLocalRandom.current().nextInt(90);

                execSql(
                    "SELECT " + grpNo + " as groupNo, * FROM IGNITE.CACHES WHERE delay(?) = ? LIMIT 1", delay, delay
                );
            }
        }, 20, "test-async-query-runner");

        {
            long minDuration = 100L;
            distCfg.topLongestQueryMinDuration(minDuration).get();

            U.sleep(2 * windowSize);

            List<TopLongestQueriesEntry> topEntries = tracker.topLongestQueries();

            assertEquals(topEntries.toString(), 2, topEntries.size());
            assertTrue("All entries should have duration >=" + minDuration + ": " + topEntries,
                topEntries.stream().allMatch(e -> e.duration() >= minDuration));

            Assert.assertThat(topEntries.get(0).query(), containsString("SELECT 2 as groupNo"));
            Assert.assertThat(topEntries.get(1).query(), containsString("SELECT 1 as groupNo"));
        }

        {
            long minDuration = 150L;
            distCfg.topLongestQueryMinDuration(150L).get();

            U.sleep(2 * windowSize);

            List<TopLongestQueriesEntry> topEntries = tracker.topLongestQueries();

            assertEquals(topEntries.toString(), 1, topEntries.size());
            assertTrue(topEntries.get(0).duration() >= minDuration);

            Assert.assertThat(topEntries.get(0).query(), containsString("SELECT 2 as groupNo"));
        }

        stopped.set(true);
        runners.get(getTestTimeout());
    }

    /**
     * Ensure tracker reflects to the window size changes:
     *    1) set a window size to a very big value (so that snapshot is not collected)
     *    2) run several queries
     *    3) verify that top queries chart is empty
     *    4) set the window size to a small value and wait
     *    5) verify that top queries chart is not empty
     *    6) wait one more time for the same amount of time
     *    7) verify that top queries chart is empty (because new qeueries is not running)
     */
    @Test
    public void testTrackerRespectsWindowChange() throws IgniteCheckedException {
        distCfg.topLongestQueryMinDuration(0L).get();

        distCfg.topLongestQueryWindowSize(Long.MAX_VALUE).get();

        for (int i = 0; i < 5; i++) {
            int rnd = ThreadLocalRandom.current().nextInt(100);

            execSql(
                "SELECT " + rnd + " as groupNo, * FROM IGNITE.CACHES WHERE delay(?) = ? LIMIT 1", rnd, rnd
            );
        }

        assertTrue(tracker.topLongestQueries().isEmpty());

        long windowSize = 100L;

        distCfg.topLongestQueryWindowSize(windowSize).get();

        List<TopLongestQueriesEntry> res = tracker.topLongestQueries();

        assertFalse(res.isEmpty());
        assertTrue(res.size() <= distCfg.topLongestQueryListSize());

        U.sleep(2 * windowSize);

        assertTrue(tracker.topLongestQueries().isEmpty());
    }

    /**
     * Execute query with given arguments.
     *
     * @param sql Sql.
     * @param args Args.
     */
    private void execSql(String sql, Object... args) {
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }
}