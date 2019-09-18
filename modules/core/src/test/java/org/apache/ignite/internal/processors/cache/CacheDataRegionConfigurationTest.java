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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class CacheDataRegionConfigurationTest extends GridCommonAbstractTest {
    /** */
    private volatile CacheConfiguration ccfg;

    /** */
    private volatile DataStorageConfiguration memCfg;


    /** */
    private IgniteLogger logger;

    /** */
    private static final long DFLT_MEM_PLC_SIZE = 10L * 1024 * 1024;

    /** */
    private static final long BIG_MEM_PLC_SIZE = 1024L * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (memCfg != null)
            cfg.setDataStorageConfiguration(memCfg);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        if (logger != null)
            cfg.setGridLogger(logger);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    private void checkStartGridException(Class<? extends Throwable> ex, String message) {
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                startGrid(0);
                return null;
            }
        }, ex, message);
    }

    /**
     * Verifies that proper exception is thrown when DataRegion is misconfigured for cache.
     */
    @Test
    public void testMissingDataRegion() {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setDataRegionName("nonExistingMemPlc");

        checkStartGridException(IgniteCheckedException.class, "Requested DataRegion is not configured");
    }

    /**
     * Verifies that {@link IgniteOutOfMemoryException} is thrown when cache is configured with too small DataRegion.
     */
    @Test
    public void testTooSmallDataRegion() throws Exception {
        memCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        dfltPlcCfg.setMaxSize(DFLT_MEM_PLC_SIZE);

        DataRegionConfiguration bigPlcCfg = new DataRegionConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(BIG_MEM_PLC_SIZE);

        memCfg.setDataRegionConfigurations(bigPlcCfg);
        memCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        boolean oomeThrown = false;

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            Throwable cause = e;

            do {
                if (cause instanceof IgniteOutOfMemoryException) {
                    oomeThrown = true;
                    break;
                }

                if (cause == null)
                    break;

                if (cause.getSuppressed() == null || cause.getSuppressed().length == 0)
                    cause = cause.getCause();
                else
                    cause = cause.getSuppressed()[0];
            }
            while (true);
        }

        if (!oomeThrown)
            fail("OutOfMemoryException hasn't been thrown");
    }

    /**
     * Verifies that with enough memory allocated adding values to cache doesn't cause any exceptions.
     */
    @Test
    public void testProperlySizedMemoryPolicy() throws Exception {
        memCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        dfltPlcCfg.setMaxSize(DFLT_MEM_PLC_SIZE);

        DataRegionConfiguration bigPlcCfg = new DataRegionConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(BIG_MEM_PLC_SIZE);

        memCfg.setDataRegionConfigurations(bigPlcCfg);
        memCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setDataRegionName("bigPlc");

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            fail("With properly sized DataRegion no exceptions are expected to be thrown.");
        }
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when swap and persistence are enabled at the same time
     * for a data region.
     */
    @Test
    public void testSetPersistenceAndSwap() {
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        // Enabling the persistence.
        invCfg.setPersistenceEnabled(true);
        // Enabling the swap space.
        invCfg.setSwapPath("/path/to/some/directory");

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setDataRegionName("ccfg");

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Filter to exclude the node from affinity nodes by its name.
     */
    private static class NodeNameNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final String filteredNode;

        /**
         * @param node Node.
         */
        private NodeNameNodeFilter(String node) {
            filteredNode = node;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.attribute("org.apache.ignite.ignite.name").toString().contains(filteredNode);
        }
    }

    /**
     * Verifies that warning message is printed to the logs if user tries to start a static cache in data region which
     * overhead (e.g. metapages for partitions) occupies more space of the region than a defined threshold.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWarningIfStaticCacheOverheadExceedsThreshold() throws Exception {
        String filteredSrvName = "srv2";

        DataRegionConfiguration smallRegionCfg = new DataRegionConfiguration();

        smallRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setPersistenceEnabled(true);

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(smallRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        CacheConfiguration<Object, Object> manyPartitionsCache = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        manyPartitionsCache.setAffinity(new RendezvousAffinityFunction(false, 4096));
        manyPartitionsCache.setNodeFilter(new NodeNameNodeFilter(filteredSrvName));

        ccfg = manyPartitionsCache;

        GridStringLogger srv0Logger = getStringLogger();
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        GridStringLogger srv1Logger = getStringLogger();
        logger = srv1Logger;

        startGrid("srv1");

        GridStringLogger srv2Logger = getStringLogger();
        logger = srv2Logger;

        startGrid(filteredSrvName);

        ignite0.cluster().active(true);

        //srv0 and srv1 print warning into the log as the threshold for cache in default cache group is broken
        GridTestUtils.assertContains(null, srv0Logger.toString(), "Cache group 'default' brings high overhead");
        GridTestUtils.assertContains(null, srv1Logger.toString(), "Cache group 'default' brings high overhead");

        //srv2 doesn't print the warning as it is filtered by node filter from affinity nodes
        GridTestUtils.assertNotContains(null, srv2Logger.toString(), "Cache group 'default' brings high overhead");
    }

    /**
     * Verifies that warning message is printed to the logs if user tries to start a dynamic cache in data region which
     * overhead (e.g. metapages for partitions) occupies more space of the region than a defined threshold.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWarningIfDynamicCacheOverheadExceedsThreshold() throws Exception {
        DataRegionConfiguration smallRegionCfg = new DataRegionConfiguration();

        smallRegionCfg.setName("smallRegion");
        smallRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        smallRegionCfg.setPersistenceEnabled(true);

        //explicit default data region configuration to test possible NPE case
        DataRegionConfiguration defaultRegionCfg = new DataRegionConfiguration();
        defaultRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setPersistenceEnabled(true);

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(defaultRegionCfg);
        memCfg.setDataRegionConfigurations(smallRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        GridStringLogger srv0Logger = getStringLogger();
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        GridStringLogger srv1Logger = getStringLogger();
        logger = srv1Logger;

        startGrid("srv1");

        ignite0.cluster().active(true);

        ignite0.createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setDataRegionName(defaultRegionCfg.getName())
                .setCacheMode(CacheMode.PARTITIONED)
                .setAffinity(new RendezvousAffinityFunction(false, 4096))
        );

        //srv0 and srv1 print warning into the log as the threshold for cache in default cache group is broken
        GridTestUtils.assertContains(null, srv0Logger.toString(), "Cache group 'default' brings high overhead");
        GridTestUtils.assertContains(null, srv1Logger.toString(), "Cache group 'default' brings high overhead");
    }

    /**
     * Verifies that warning is printed out to logs if after removing nodes from baseline
     * some caches reach or cross dangerous limit of metainformation overhead per data region.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key=IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value="false")
    public void testWarningOnBaselineTopologyChange() throws Exception {
        DataRegionConfiguration defaultRegionCfg = new DataRegionConfiguration();
        defaultRegionCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        defaultRegionCfg.setPersistenceEnabled(true);

        memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(defaultRegionCfg);
        //one hour to guarantee that checkpoint will be triggered by 'dirty pages amount' trigger
        memCfg.setCheckpointFrequency(60 * 60 * 1000);

        GridStringLogger srv0Logger = getStringLogger();
        logger = srv0Logger;

        IgniteEx ignite0 = startGrid("srv0");

        GridStringLogger srv1Logger = getStringLogger();
        logger = srv1Logger;

        startGrid("srv1");

        ignite0.cluster().active(true);

        ignite0.createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setDataRegionName(defaultRegionCfg.getName())
                .setCacheMode(CacheMode.PARTITIONED)
                .setAffinity(new RendezvousAffinityFunction(false, 512))
        );

        GridTestUtils.assertNotContains(null, srv0Logger.toString(), "Cache group 'default' brings high overhead");
        GridTestUtils.assertNotContains(null, srv1Logger.toString(), "Cache group 'default' brings high overhead");

        stopGrid("srv1");

        long topVer = ignite0.cluster().topologyVersion();

        ignite0.cluster().setBaselineTopology(topVer);

        awaitPartitionMapExchange();

        GridTestUtils.assertContains(null, srv0Logger.toString(), "Cache group 'default' brings high overhead");
    }

    /** */
    private GridStringLogger getStringLogger() {
        GridStringLogger strLog = new GridStringLogger(false, null);

        strLog.logLength(1024 * 1024);

        return strLog;
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when page eviction threshold is less than 0.5.
     */
    @Test
    public void testSetSmallInvalidEviction() {
        final double SMALL_EVICTION_THRESHOLD = 0.1D;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the page eviction threshold less than 0.5
        invCfg.setEvictionThreshold(SMALL_EVICTION_THRESHOLD);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when page eviction threshold is greater than 0.999.
     */
    @Test
    public void testSetBigInvalidEviction() {
        final double BIG_EVICTION_THRESHOLD = 1.0D;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the page eviction threshold greater than 0.999
        invCfg.setEvictionThreshold(BIG_EVICTION_THRESHOLD);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when empty pages pool size is less than 10
     */
    @Test
    public void testInvalidSmallEmptyPagesPoolSize() {
        final int SMALL_PAGES_POOL_SIZE = 5;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the pages pool size less than 10
        invCfg.setEmptyPagesPoolSize(SMALL_PAGES_POOL_SIZE);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when empty pages pool size is greater than
     * DataRegionConfiguration.getMaxSize() / DataStorageConfiguration.getPageSize() / 10.
     */
    @Test
    public void testInvalidBigEmptyPagesPoolSize() {
        final int DFLT_PAGE_SIZE = 1024;
        long expectedMaxPoolSize;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);
        memCfg.setPageSize(DFLT_PAGE_SIZE);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        expectedMaxPoolSize = invCfg.getMaxSize() / memCfg.getPageSize() / 10;

        if (expectedMaxPoolSize < Integer.MAX_VALUE) {
            // Setting the empty pages pool size greater than
            // DataRegionConfiguration.getMaxSize() / DataStorageConfiguration.getPageSize() / 10
            invCfg.setEmptyPagesPoolSize((int)expectedMaxPoolSize + 1);
            memCfg.setDataRegionConfigurations(invCfg);
            checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
        }
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when IgniteCheckedException if validation of
     * memory metrics properties fails. Metrics rate time interval must not be less than 1000ms.
     */
    @Test
    public void testInvalidMetricsProperties() {
        final long SMALL_RATE_TIME_INTERVAL_MS = 999;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the metrics rate time less then 1000ms
        invCfg.setMetricsRateTimeInterval(SMALL_RATE_TIME_INTERVAL_MS);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }

    /**
     * Verifies that {@link IgniteCheckedException} is thrown when IgniteCheckedException if validation of
     * memory metrics properties fails. Metrics sub interval count must be positive.
     */
    @Test
    public void testInvalidSubIntervalCount() {
        final int NEG_SUB_INTERVAL_COUNT = -1000;
        DataRegionConfiguration invCfg = new DataRegionConfiguration();

        invCfg.setName("invCfg");
        invCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        invCfg.setMaxSize(DFLT_MEM_PLC_SIZE);
        invCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        // Setting the metrics sub interval count as negative
        invCfg.setMetricsSubIntervalCount(NEG_SUB_INTERVAL_COUNT);

        memCfg = new DataStorageConfiguration();
        memCfg.setDataRegionConfigurations(invCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        checkStartGridException(IgniteCheckedException.class, "Failed to start processor: GridProcessorAdapter []");
    }
}
