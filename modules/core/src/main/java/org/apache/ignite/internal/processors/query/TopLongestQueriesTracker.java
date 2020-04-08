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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.internal.A;

/** */
public class TopLongestQueriesTracker {
    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Executer used to collect snapshots on schedule. */
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    /**
     * Queue of the queries sorted by ascending duration. Used to drop
     * shortest queries when max size is reached.
     */
    private final PriorityBlockingQueue<TopLongestQueriesEntry> queriesQueue = new PriorityBlockingQueue<>(
        16, Comparator.comparingLong(TopLongestQueriesEntry::duration)
    );

    /** Mapping qury to its duration. Used to handle dublicates. */
    private final ConcurrentMap<EntryKey, TopLongestQueriesEntry> qryToDurationMap = new ConcurrentHashMap<>();

    /** */
    private final Supplier<Long> currTimeSupplier;

    /** Time when last snapshot was collected. */
    private volatile long lastSnapshotTime;

    /** Maximum size of the top query chart. */
    private volatile int longestQueriesListSize;

    /** Minimal duration that tracker should collect. */
    private volatile long minDuration;

    /** */
    private ScheduledFuture<?> task;

    /** */
    private volatile List<TopLongestQueriesEntry> lastSnapshot = Collections.emptyList();

    /**
     * @param currTimeSupplier Current time supplier.
     */
    public TopLongestQueriesTracker(Supplier<Long> currTimeSupplier, long minDuration, int longestQueriesListSize, long windowSize) {
        A.ensure(currTimeSupplier != null, "currTimeSupplier != null");

        this.currTimeSupplier = currTimeSupplier;

        lastSnapshotTime = currTimeSupplier.get();

        longestQueriesListSize(longestQueriesListSize);
        minDuration(minDuration);
        windowSize(windowSize);
    }

    /**
     * @param windowSize Window size.
     */
    public synchronized void windowSize(long windowSize) {
        if (task != null)
            task.cancel(false);

        if (windowSize <= 0)
            return;

        long delay = Math.max(windowSize + lastSnapshotTime - currTimeSupplier.get(), 0);

        task = executor.scheduleAtFixedRate(this::closeSnapshot, delay, windowSize, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops internal executor.
     */
    public void stop() {
        executor.shutdownNow();
    }

    /**
     * @param minDuration Min duration.
     */
    public void minDuration(long minDuration) {
        A.ensure(minDuration >= 0, "minDuration >= 0");

        this.minDuration = minDuration;
    }

    /**
     * @param qryInfo Query info.
     */
    public void collect(GridRunningQueryInfo qryInfo) {
        final long duration = currTimeSupplier.get() - qryInfo.startTime();

        if (duration < minDuration)
            return;

        TopLongestQueriesEntry fastest;
        if (queriesQueue.size() >= longestQueriesListSize && (fastest = queriesQueue.peek()) != null && fastest.duration() > duration)
            return;

        lock.readLock().lock();

        try {
            TopLongestQueriesEntry oldEntry, newEntry = entryFromRunningInfo(qryInfo, duration);
            EntryKey newEntryKey = extractKey(newEntry);
            do {
                oldEntry = qryToDurationMap.get(newEntryKey);

                // such query is already registered with bigger duration
                if (oldEntry != null && oldEntry.duration() >= duration)
                    return;

            } while (
                oldEntry == null ? qryToDurationMap.putIfAbsent(newEntryKey, newEntry) != null : !qryToDurationMap.replace(newEntryKey, oldEntry, newEntry)
            );

            queriesQueue.add(newEntry);

            if (oldEntry != null)
                queriesQueue.remove(oldEntry);

            else if (queriesQueue.size() > longestQueriesListSize) {
                fastest = queriesQueue.poll();

                qryToDurationMap.remove(extractKey(fastest), fastest);
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** */
    void closeSnapshot() {
        lastSnapshotTime = currTimeSupplier.get();

        List<TopLongestQueriesEntry> list = new ArrayList<>();

        lock.writeLock().lock();

        try {
            queriesQueue.drainTo(list);

            qryToDurationMap.clear();
        }
        finally {
            lock.writeLock().unlock();
        }

        list.subList(0, Math.min(0, longestQueriesListSize - list.size())).clear();

        list.sort(Comparator.comparingLong(TopLongestQueriesEntry::duration).reversed());

        lastSnapshot = list;
    }

    /**
     * @return Latests snapshot of top longest queries.
     */
    public List<TopLongestQueriesEntry> topLongestQueries() {
        return Collections.unmodifiableList(lastSnapshot);
    }

    /**
     * @param longestQueriesListSize Longest queries list size.
     */
    public void longestQueriesListSize(int longestQueriesListSize) {
        A.ensure(longestQueriesListSize > 0, "longestQueriesListSize > 0");

        this.longestQueriesListSize = longestQueriesListSize;
    }

    /**
     * @param qryInfo Query info.
     * @param duration Duration.
     */
    private TopLongestQueriesEntry entryFromRunningInfo(GridRunningQueryInfo qryInfo, long duration) {
        return new TopLongestQueriesEntry(
            qryInfo.schemaName(), qryInfo.query(), qryInfo.local(), qryInfo.startTime(), duration
        );
    }

    /**
     * @param entry Entry.
     */
    private EntryKey extractKey(TopLongestQueriesEntry entry) {
        return new EntryKey(entry.schema(), entry.query(), entry.local());
    }

    /**
     * Part of {@link TopLongestQueriesEntry} that allows to distinguish between queries.
     */
    private static class EntryKey {
        /** */
        private final String schema;

        /** */
        private final String qry;

        /** */
        private final boolean loc;

        /** */
        private final int hash;

        /**
         * @param schema Schema.
         * @param qry Query.
         * @param loc Local.
         */
        public EntryKey(String schema, String qry, boolean loc) {
            this.schema = Objects.requireNonNull(schema, "schema");
            this.qry = Objects.requireNonNull(qry, "qry");;
            this.loc = loc;

            hash = 31 * (31 * schema.hashCode() + qry.hashCode()) + (loc ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            EntryKey key = (EntryKey)o;
            return loc == key.loc &&
                schema.equals(key.schema) &&
                qry.equals(key.qry);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }
}
