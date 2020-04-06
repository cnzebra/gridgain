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

/** */
public class TopLongestQueriesTracker {
    /** */
    private volatile long lastSnapshotTime;

    /** */
    private volatile int longestQueriesListSize;

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    /** */
    private final Supplier<Long> currTimeSupplier;

    /** Min duration. */
    private volatile long minDuration;

    /** */
    private ScheduledFuture<?> task;

    /** */
    private volatile List<TopLongestQueriesEntry> lastSnapshot = Collections.emptyList();

    /** Cur. */
    private final PriorityBlockingQueue<TopLongestQueriesEntry> cur = new PriorityBlockingQueue<>(
        16, Comparator.comparingLong(TopLongestQueriesEntry::duration)
    );

    /** Map. */
    private final ConcurrentMap<EntryKey, TopLongestQueriesEntry> qryToDurationMap = new ConcurrentHashMap<>();

    /**
     * @param currTimeSupplier Current time supplier.
     */
    public TopLongestQueriesTracker(Supplier<Long> currTimeSupplier, long minDuration, int longestQueriesListSize) {
        this.currTimeSupplier = currTimeSupplier;
        this.minDuration = minDuration;
        this.longestQueriesListSize = longestQueriesListSize;
    }

    /**
     * @param windowSize Window size.
     */
    public void windowSize(long windowSize) {
        if (task != null)
            task.cancel(false);

        long delay = Math.min(windowSize + lastSnapshotTime - currTimeSupplier.get(), 0);

        task = executor.scheduleAtFixedRate(this::closeSnapshot, delay, windowSize, TimeUnit.MILLISECONDS);
    }

    /**
     * @param minDuration Min duration.
     */
    public void minDuration(long minDuration) {
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
        if (cur.size() >= longestQueriesListSize && (fastest = cur.peek()) != null && fastest.duration() > duration)
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

            cur.add(newEntry);

            if (oldEntry != null)
                cur.remove(oldEntry);

            else if (cur.size() > longestQueriesListSize) {
                fastest = cur.poll();

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
            cur.drainTo(list);

            qryToDurationMap.clear();
        }
        finally {
            lock.writeLock().unlock();
        }

        list.subList(0, Math.min(0, longestQueriesListSize - list.size())).clear();

        list.sort(Comparator.comparingLong(TopLongestQueriesEntry::duration).reversed());

        lastSnapshot = list;
    }

    /** */
    public List<TopLongestQueriesEntry> topLongestQueries() {
        return Collections.unmodifiableList(lastSnapshot);
    }

    /**
     * @param longestQueriesListSize Longest queries list size.
     */
    public void longestQueriesListSize(int longestQueriesListSize) {
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
     * Part of {@link TopLongestQueriesEntry} that allows to distinguish between requests.
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
