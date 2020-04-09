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

package org.apache.ignite.ml.recommendation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link RecommendationTrainer}. */
public class RecommendationTrainerTest {
    /** */
    @Test
    public void testFit() {
        int size = 100;
        Random rnd = new Random(0L);
        Double[][] ratings = new Double[size][size];
        // Quadrant I contains "0", quadrant II contains "1", quadrant III contains "0", quadrant IV contains "1".
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (rnd.nextBoolean())
                    ratings[i][j] = ((i > size / 2) ^ (j > size / 2)) ? 1.0 : 0.0;
            }
        }

        int seq = 0;
        Map<Integer, ObjectSubjectRatingTriplet<Integer, Integer>> data = new HashMap<>();
        for (ObjectSubjectRatingTriplet<Integer, Integer> triplet : toList(ratings))
            data.put(seq++, triplet);

        RecommendationTrainer trainer = new RecommendationTrainer()
            .withLearningRate(50.0)
            .withBatchSize(10)
            .withK(2)
            .withMaxIterations(-1)
            .withMinMdlImprovement(0.5)
            .withLearningEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1))
            .withTrainerEnvironment(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1).buildForTrainer());

        RecommendationModel<Integer, Integer> mdl = trainer.fit(new LocalDatasetBuilder<>(data, 10));

        int incorrect = 0;
        for (ObjectSubjectRatingTriplet<Integer, Integer> triplet : toList(ratings)) {
            double prediction = Math.round(mdl.predict(triplet));
            if (Math.abs(prediction - triplet.getRating()) >= 1e-5) incorrect++;
        }

        assertEquals(0, incorrect);
    }

    /**
     * Converts rating matrix to list of {@link ObjectSubjectRatingTriplet} objects.
     *
     * @param ratings Rating matrix.
     * @return List of {@link ObjectSubjectRatingTriplet} objects.
     */
    private static List<ObjectSubjectRatingTriplet<Integer, Integer>> toList(Double[][] ratings) {
        List<ObjectSubjectRatingTriplet<Integer, Integer>> res = new ArrayList<>();

        for (int i = 0; i < ratings.length; i++) {
            for (int j = 0; j < ratings[i].length; j++) {
                if (ratings[i][j] != null)
                    res.add(new ObjectSubjectRatingTriplet<>(i, j, ratings[i][j]));
            }
        }

        return res;
    }
}
