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

package org.apache.ignite.spi.checkpoint.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.session.GridSessionCheckpointAbstractSelfTest;
import org.apache.ignite.session.GridSessionCheckpointSelfTest;
import org.apache.ignite.testsuites.IgniteS3TestSuite;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Grid session checkpoint self test using {@link S3CheckpointSpi}.
 */
public class S3SessionCheckpointSelfTest extends GridSessionCheckpointAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Test
    public void testS3Checkpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        S3CheckpointSpi spi = new S3CheckpointSpi();

        AWSCredentials cred = new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey());

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix(S3CheckpointSpiSelfTest.getBucketNameSuffix());

        cfg.setCheckpointSpi(spi);

        GridSessionCheckpointSelfTest.spi = spi;

        checkCheckpoints(cfg);
    }
}
