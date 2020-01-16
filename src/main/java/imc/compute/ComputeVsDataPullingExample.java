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

package imc.compute;

import imc.NodeStartup;
import imc.model.SampleRecord;
import java.sql.Date;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Example demonstrates performance advantages of distributed computations that let us execute custom logic
 * on the server nodes with minimal data movement over the network. The compute tasks based approach is compared to
 * key-value APIs based method that have to bring data from the servers to the example app via the network.
 * <br>
 * You can run the demo on your local laptop or spin up a remote cluster with larger capacity. Adjust
 * {@param RECORDS_COUNT} parameter to load a different number of records (an average record size is around 1
 * kilobyte). The bigger {@param RECORDS_COUNT} the worse will be latency of key-value based methods as long as much
 * more data is to be sent over the network interfaces. While the latency of the compute-based approach will stay
 * relatively the same.
 * <br>
 * Start 2+ server nodes with {@link NodeStartup} class or from a terminal window with
 * {@code ./ignite.sh {parent_directory}/ImcEssentials/config/ignite-configuration.xml} command.
 */
public class ComputeVsDataPullingExample {
    /**
     * Adjust to control a number of the records to be loaded.
     */
    private static int RECORDS_CNT = 100_000;

    /**
     * Connection to the server nodes cluster.
     */
    private static Ignite clientConnection;

    /**
     * Starting the example.
     * @param args
     */
    public static void main(String[] args) {
        Ignition.setClientMode(true);

        clientConnection = Ignition.start("config/ignite-configuration.xml");

        System.out.println(">>> Connected to the cluster");

        // Starting the cache and loading data.
        IgniteCache<Integer, SampleRecord> recordsCache = startAndLoadRecordsCache(clientConnection);


        // Calculating with a brute-force cache.get(key) method - the slowest approach
        calcByPullingDataFromCluster(recordsCache);

        // Calculating with cache.getAll(keys) that reduces network roundtrips count
        calcByPullingDataInBulkFromCluster(recordsCache);

        // Calculating with compute task avoiding all the data movement over the network
        caclWithComputeTask(recordsCache);

        // Closing client connection
        clientConnection.close();
    }

    /**
     * Starts the cache and loads it with records.
     *
     * @param clientConnection Connection to the cluster.
     * @return Ignite cache.
     */
    private static IgniteCache<Integer, SampleRecord> startAndLoadRecordsCache(Ignite clientConnection) {
        CacheConfiguration<Integer, SampleRecord> cacheCfg = new CacheConfiguration<>("RecordsCache");

        IgniteCache<Integer, SampleRecord> recordsCache = clientConnection.getOrCreateCache(cacheCfg);

        System.out.println(">>> Created cache for records");

        if (recordsCache.size() == RECORDS_CNT) {
            System.out.println(">>> Skipping loading, cache was created before");

            return recordsCache;
        }

        recordsCache.clear();

        IgniteDataStreamer<Integer, SampleRecord> loader = clientConnection.dataStreamer(recordsCache.getName());

        Random randomDistance = new Random();
        Random randomTemp = new Random();

        for (int i = 0; i < RECORDS_CNT; i++)
            loader.addData(i, new SampleRecord(randomDistance.nextInt(100_000), randomTemp.nextFloat(),
                new Date(System.currentTimeMillis())));

        loader.flush();

        System.out.println(">>> Loaded " + RECORDS_CNT + " records");

        return recordsCache;
    }

    /**
     * Executing our custom calculation by pulling data from the cluster with key-value calls. A number of network
     * roundtrips will be equal to the number of records stored in the cluster.
     *
     * @param recordsCache Ignite cache instance.
     */
    private static void calcByPullingDataFromCluster(IgniteCache<Integer, SampleRecord> recordsCache) {
        long startTime = System.currentTimeMillis();

        float highestTemp = 0;

        long longestDistance = 0;

        for (int i = 0; i < RECORDS_CNT; i++) {
            SampleRecord record = recordsCache.get(i);

            if (record.getDistance() > longestDistance)
                longestDistance = record.getDistance();

            if (record.getTemperature() > highestTemp)
                highestTemp = record.getTemperature();

            //Running other custom logic...
        }

        System.out.println(">>> Finished calculation with get(key) method [temp = " + highestTemp + ", distance = " +
            longestDistance + ", execution time = " + (System.currentTimeMillis() - startTime) + " millis]");
    }

    /**
     * Executing the same logic but doing fewer network roundtrips. The {@code getAll} method will allow us to reduce
     * a number of the network calls from {@param RECORDS_CNT} to a few. However, all the data is still transferred
     * over the network. If you still need to transfer data from the cluster for your app then consider
     * {@link IgniteCache#invokeAll(Set, EntryProcessor, Object...)} as another optimization option that can return
     * specific fields instead of full records.
     *
     * @param recordsCache Ignite cache instance.
     */
    private static void calcByPullingDataInBulkFromCluster(IgniteCache<Integer, SampleRecord> recordsCache) {
        long startTime = System.currentTimeMillis();

        float highestTemp = 0;

        long longestDistance = 0;

        Set<Integer> keys = IntStream.range(0, RECORDS_CNT).boxed().collect(Collectors.toSet());

        Collection<SampleRecord> records = recordsCache.getAll(keys).values();


        Iterator<SampleRecord> recordsIterator = records.iterator();

        while (recordsIterator.hasNext()) {
            SampleRecord record = recordsIterator.next();

            if (record.getDistance() > longestDistance)
                longestDistance = record.getDistance();

            if (record.getTemperature() > highestTemp)
                highestTemp = record.getTemperature();

            //Running other custom logic...
        }

        System.out.println(">>> Finished calculation with getAll(keys) method [temp = " + highestTemp + ", distance =" +
            " " +
            longestDistance + ", execution time = " + (System.currentTimeMillis() - startTime) + " millis]");
    }

    /**
     * Wrapping the login in an Ignite compute task and executing it in-place on the server nodes. This method avoids
     * all records shuffling or movement of the network. Records are stored on the server and we bring our logic there.
     *
     * @param recordsCache Ignite cache instance.
     */
    private static void caclWithComputeTask(IgniteCache<Integer, SampleRecord> recordsCache) {
        long startTime = System.currentTimeMillis();

        IgniteCompute compute = clientConnection.compute();

        String cacheName = recordsCache.getName();

        // Hint for prod usage: you might need to create a Class for your computation as long as anonymous classes
        // such as the one below (instance of IgniteCallable) will serialize the outer class
        // (ComputeVsDataPullingExample) and transfer it to the servers. It's not a problem for this example.
        Collection<Object[]> resultsFromServers = compute.broadcast(new IgniteCallable<Object[]>() {
            @IgniteInstanceResource
            Ignite clientConnection;

            float highestTemp = 0;

            long longestDistance = 0;

            @Override public Object[] call() throws Exception {
                IgniteCache<Integer, BinaryObject> recordsCache = clientConnection.cache(cacheName).withKeepBinary();

                ScanQuery<Integer, BinaryObject> scanQuery = new ScanQuery<>();

                // Hint for prod usage: consider using 'affinityRun/Call' methods to ensure the data is not fully
                // rebalanced from the server node until a compute task is completed.
                // Read more here:
                // https://www.gridgain.com/docs/latest/developers-guide/collocated-computations#collocating-computations-with-data
                scanQuery.setLocal(true);

                QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = recordsCache.query(scanQuery);

                cursor.forEach(new Consumer<Cache.Entry<Integer, BinaryObject>>() {
                    @Override public void accept(Cache.Entry<Integer, BinaryObject> entry) {
                        BinaryObject record = entry.getValue();

                        if ((long)record.field("distance") > longestDistance)
                            longestDistance = record.field("distance");

                        if ((float)record.field("temperature") > highestTemp)
                            highestTemp = record.field("temperature");

                        //Running other custom logic...
                    }
                });

                Object[] result = new Object[] {highestTemp, longestDistance};

                System.out.println(">>> Finished local calculation [temp = " + result[0] + ", distance = " +
                    result[1] + "]");

                return result;
            }
        });

        float highestTemp = 0;

        long longestDistance = 0;

        Iterator<Object[]> partialResults = resultsFromServers.iterator();

        while (partialResults.hasNext()) {
            Object[] result = partialResults.next();

            if ((long)result[1] > longestDistance)
                longestDistance = (long)result[1];

            if ((float)result[0] > highestTemp)
                highestTemp = (float)result[0];
        }

        System.out.println(">>> Finished calculation with compute task [temp = " + highestTemp + ", distance = " +
            longestDistance + ", execution time = " + (System.currentTimeMillis() - startTime) + " millis]");
    }
}
