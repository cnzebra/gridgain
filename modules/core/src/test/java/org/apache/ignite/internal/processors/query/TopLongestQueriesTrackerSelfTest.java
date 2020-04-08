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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

/** */
public class TopLongestQueriesTrackerSelfTest extends GridCommonAbstractTest {
    /** */
    private final AtomicLong currTime = new AtomicLong();

    /** */
    @Before
    public void setUp() {
        currTime.set(0);
    }

    /**
     * Verify that queries with duration less than {@code minDuration}
     * won't be collected by the tracker.
     */
    @Test
    public void testTrackerIgnoresShortQueries() {
        TopLongestQueriesTracker tracker = new TopLongestQueriesTracker(currTime::get, 10, 3, 0);

        for (long i = 0; i < 7; i++) {
            currTime.set(5L + i);
            tracker.collect(createRunningQryInfo("q" + i, 0L));
        }

        tracker.closeSnapshot();

        compareEntry(Arrays.asList(
            createTopLongestQueriesEntry("q6", 11),
            createTopLongestQueriesEntry("q5", 10)
        ), tracker.topLongestQueries());
    }

    /**
     * Verify that queries with decreased duration is collected
     * until {@code longestQueriesListSize} is reached.
     */
    @Test
    public void testTrackerCollectsDecreasedQueriesUntilLimitIsReached() {
        TopLongestQueriesTracker tracker = new TopLongestQueriesTracker(currTime::get, 0, 3, 0);

        for (long i = 6; i >= 0; i--) {
            currTime.set(i);
            tracker.collect(createRunningQryInfo("q" + i, 0L));
        }

        tracker.closeSnapshot();

        compareEntry(Arrays.asList(
            createTopLongestQueriesEntry("q6", 6),
            createTopLongestQueriesEntry("q5", 5),
            createTopLongestQueriesEntry("q4", 4)
        ), tracker.topLongestQueries());
    }

    /**
     * Verify that same queries is not dublicated and stored only the longest one.
     */
    @Test
    public void testTrackerProperHandlesDublicatedQueries() {
        TopLongestQueriesTracker tracker = new TopLongestQueriesTracker(currTime::get, 0, 10, 0);

        currTime.set(10);

        // the shortest run
        tracker.collect(createRunningQryInfo("q1", 9L));
        tracker.collect(createRunningQryInfo("q2", 9L));

        // the longest run, should override previos values
        tracker.collect(createRunningQryInfo("q1", 1L));
        tracker.collect(createRunningQryInfo("q2", 0L));

        // not longest run, should be ignored
        tracker.collect(createRunningQryInfo("q1", 5L));
        tracker.collect(createRunningQryInfo("q2", 5L));

        tracker.closeSnapshot();

        compareEntry(Arrays.asList(
            createTopLongestQueriesEntry("q2", 10),
            createTopLongestQueriesEntry("q1", 9)
        ), tracker.topLongestQueries());
    }

    /**
     * Verify that same queries is not dublicated and stored only the longest one.
     */
    @Test
    public void testTrackerHandlesConcurrentInvocation() throws InterruptedException {
        final int threadCnt = 5;
        final int entriesPerThread = 20_000;
        final int allFinishedAt = 10_000;

        List<GridRunningQueryInfo> infos = new ArrayList<>(entriesPerThread);

        for (int i = 0; i < (entriesPerThread - 1) * threadCnt; i++) {
            infos.add(
                createRunningQryInfo(
                    "qry" + (i % 1000),
                    // should be greater than 0 because longest query will be added after,
                    // and lower allFinishedAt to prevent negative durations
                    100 + ThreadLocalRandom.current().nextInt(allFinishedAt - 150))
            );
        }

        for (int i = 0; i < threadCnt; i++)
            infos.add(createRunningQryInfo("qry" + (5 * (i + 1)), i));

        Collections.shuffle(infos);

        List<Thread> threads = new ArrayList<>(threadCnt);

        TopLongestQueriesTracker tracker = new TopLongestQueriesTracker(() -> (long)allFinishedAt, 0, threadCnt, 0);

        for (int i = 0; i < threadCnt; i++) {
            final int threadNo = i;
            threads.add(new Thread(() -> {
                for (GridRunningQueryInfo info : infos.subList(threadNo * entriesPerThread, (threadNo + 1) * entriesPerThread))
                    tracker.collect(info);
            }));
        }

        threads.forEach(Thread::start);

        for (Thread thread : threads)
            thread.join();

        tracker.closeSnapshot();

        List<TopLongestQueriesEntry> exp = new ArrayList<>(threadCnt);

        for (int i = 0; i < threadCnt; i++)
            exp.add(createTopLongestQueriesEntry("qry" + (5 * (i + 1)), allFinishedAt - i));

        compareEntry(exp, tracker.topLongestQueries());
    }

    /**
     * @param qry Query.
     * @param startTime Start time.
     */
    private GridRunningQueryInfo createRunningQryInfo(String qry, long startTime) {
        return new GridRunningQueryInfo(
            0L, UUID.randomUUID(), qry, GridCacheQueryType.SQL, qry, startTime, null, false, null, "test"
        );
    }

    /**
     * @param qry Query.
     * @param duration Duration.
     */
    private TopLongestQueriesEntry createTopLongestQueriesEntry(String qry, long duration) {
        return new TopLongestQueriesEntry(
            qry, qry, false, 0L, duration
        );
    }

    /**
     * Compares two entries. Throws asserion eeror in case enttries is not
     * the same.
     *
     * @param exp Expected.
     * @param actual Actual.
     */
    private void compareEntry(TopLongestQueriesEntry exp, TopLongestQueriesEntry actual) {
        if (!compareEntry0(exp, actual))
            fail("Exp: " + exp + ", act: " + actual);
    }

    /**
     * Compares two entries. Throws asserion eeror in case enttries is not
     * the same.
     *
     * @param exp Expected.
     * @param actual Actual.
     */
    private void compareEntry(Collection<TopLongestQueriesEntry> exp, Collection<TopLongestQueriesEntry> actual) {
        if (exp.size() != actual.size())
            fail("Exp: " + exp + ", act: " + actual);

        Iterator<TopLongestQueriesEntry> expI = exp.iterator();
        Iterator<TopLongestQueriesEntry> actI = actual.iterator();

        while (expI.hasNext()) {
            if (!compareEntry0(expI.next(), actI.next()))
                fail("Exp: " + exp + ", act: " + actual);
        }
    }

    /**
     * Compares two entries.
     *
     * @param exp Expected.
     * @param actual Actual.
     * @return {@code true} if entries are equal.
     */
    private boolean compareEntry0(TopLongestQueriesEntry exp, TopLongestQueriesEntry actual) {
        return exp.schema().equals(actual.schema())
            && exp.query().equals(actual.schema())
            && exp.local() == actual.local()
            && exp.duration() == actual.duration();
    }
}