package imc.ml;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.regressions.OLSMultipleLinearRegression;
import org.apache.ignite.thread.IgniteThread;


/**
 * Run linear regression over distributed matrix.
 *
 *
 * @see OLSMultipleLinearRegression
 */
public class LinearRegressionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws InterruptedException {
        // Connecting in the client mode.
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/ignite-configuration.xml")) {
            System.out.println();
            System.out.println(">>> Linear regression over sparse distributed matrix API usage example started.");

            // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
            // because we create ignite cache internally.
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                LinearRegressionExample.class.getSimpleName(), () -> {

                double[] data = {
                    8, 78, 284, 9.100000381, 109,
                    9.300000191, 68, 433, 8.699999809, 144,
                    7.5, 70, 739, 7.199999809, 113,
                    8.899999619, 96, 1792, 8.899999619, 97,
                    10.19999981, 74, 477, 8.300000191, 206,
                    8.300000191, 111, 362, 10.89999962, 124,
                    8.800000191, 77, 671, 10, 152,
                    8.800000191, 168, 636, 9.100000381, 162,
                    10.69999981, 82, 329, 8.699999809, 150,
                    11.69999981, 89, 634, 7.599999905, 134,
                    8.5, 149, 631, 10.80000019, 292,
                    8.300000191, 60, 257, 9.5, 108,
                    8.199999809, 96, 284, 8.800000191, 111,
                    7.900000095, 83, 603, 9.5, 182,
                    10.30000019, 130, 686, 8.699999809, 129,
                    7.400000095, 145, 345, 11.19999981, 158,
                    9.600000381, 112, 1357, 9.699999809, 186,
                    9.300000191, 131, 544, 9.600000381, 177,
                    10.60000038, 80, 205, 9.100000381, 127,
                    9.699999809, 130, 1264, 9.199999809, 179,
                    11.60000038, 140, 688, 8.300000191, 80,
                    8.100000381, 154, 354, 8.399999619, 103,
                    9.800000191, 118, 1632, 9.399999619, 101,
                    7.400000095, 94, 348, 9.800000191, 117,
                    9.399999619, 119, 370, 10.39999962, 88,
                    11.19999981, 153, 648, 9.899999619, 78,
                    9.100000381, 116, 366, 9.199999809, 102,
                    10.5, 97, 540, 10.30000019, 95,
                    11.89999962, 176, 680, 8.899999619, 80,
                    8.399999619, 75, 345, 9.600000381, 92,
                    5, 134, 525, 10.30000019, 126,
                    9.800000191, 161, 870, 10.39999962, 108,
                    9.800000191, 111, 669, 9.699999809, 77,
                    10.80000019, 114, 452, 9.600000381, 60,
                    10.10000038, 142, 430, 10.69999981, 71,
                    10.89999962, 238, 822, 10.30000019, 86,
                    9.199999809, 78, 190, 10.69999981, 93,
                    8.300000191, 196, 867, 9.600000381, 106,
                    7.300000191, 125, 969, 10.5, 162,
                    9.399999619, 82, 499, 7.699999809, 95,
                    9.399999619, 125, 925, 10.19999981, 91,
                    9.800000191, 129, 353, 9.899999619, 52,
                    3.599999905, 84, 288, 8.399999619, 110,
                    8.399999619, 183, 718, 10.39999962, 69,
                    10.80000019, 119, 540, 9.199999809, 57,
                    10.10000038, 180, 668, 13, 106,
                    9, 82, 347, 8.800000191, 40,
                    10, 71, 345, 9.199999809, 50,
                    11.30000019, 118, 463, 7.800000191, 35,
                    11.30000019, 121, 728, 8.199999809, 86,
                    12.80000019, 68, 383, 7.400000095, 57,
                    10, 112, 316, 10.39999962, 57,
                    6.699999809, 109, 388, 8.899999619, 94
                };

                final int nobs = 53;
                final int nvars = 4;

                System.out.println(">>> Create new SparseDistributedMatrix inside IgniteThread.");
                // Create SparseDistributedMatrix, new cache will be created automagically.
                SparseDistributedMatrix distributedMatrix = new SparseDistributedMatrix(0, 0,
                    StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                System.out.println(">>> Create new linear regression object");
                OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
                regression.newSampleData(data, nobs, nvars, distributedMatrix);
                System.out.println();

                System.out.println(">>> Estimates the regression parameters b:");
                System.out.println(Arrays.toString(regression.estimateRegressionParameters()));

                System.out.println(">>> Estimates the residuals, ie u = y - X*b:");
                System.out.println(Arrays.toString(regression.estimateResiduals()));

                System.out.println(">>> Standard errors of the regression parameters:");
                System.out.println(Arrays.toString(regression.estimateRegressionParametersStandardErrors()));

                System.out.println(">>> Estimates the variance of the regression parameters, ie Var(b):");
                Tracer.showAscii(regression.estimateRegressionParametersVariance());

                System.out.println(">>> Estimates the standard error of the regression:");
                System.out.println(regression.estimateRegressionStandardError());

                System.out.println(">>> R-Squared statistic:");
                System.out.println(regression.calculateRSquared());

                System.out.println(">>> Adjusted R-squared statistic:");
                System.out.println(regression.calculateAdjustedRSquared());

                System.out.println(">>> Returns the variance of the regressand, ie Var(y):");
                System.out.println(regression.estimateErrorVariance());
            });

            igniteThread.start();

            igniteThread.join();
        }
    }

}