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

package imc.keyvalue;

import imc.NodeStartup;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Demonstrates how cache can be populated with data utilizing {@link IgniteDataStreamer} API.
 * {@link IgniteDataStreamer} is a lot more efficient to use than standard
 * {@code put(...)} operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} config/ignite-configuration.xml'}.
 * <p>
 * Alternatively you can run {@link NodeStartup} in another JVM which will
 * start node with {@code config/ignite-configuration.xml} configuration.
 */
public class DataStreamerExample {
    /** Cache name. */
    private static final String CACHE_NAME = DataStreamerExample.class.getSimpleName();

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 500000;

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 512 * 1024 * 1024;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        checkMinMemory(MIN_MEMORY);

        // Connecting in the client mode.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/ignite-configuration.xml")) {
            System.out.println();
            System.out.println(">>> Cache data streamer example started.");

            CacheConfiguration cacheCfg = new CacheConfiguration(CACHE_NAME);

            // Setting a replication factor to 1.
            cacheCfg.setBackups(1);

            // Cache to preload data to.
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheCfg);
            long start = System.currentTimeMillis();

            try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer(CACHE_NAME)) {
                // Configure loader.
                stmr.perNodeBufferSize(1024);
                stmr.perNodeParallelOperations(8);

                for (int i = 0; i < ENTRY_COUNT; i++) {
                    stmr.addData(i, Integer.toString(i));

                    // Print out progress while loading cache.
                    if (i > 0 && i % 10000 == 0)
                        System.out.println("Loaded " + i + " keys.");
                }
            }

            long end = System.currentTimeMillis();

            System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " + (end - start) + "ms.");
        }
    }

    private static void checkMinMemory(long min) {
        long maxMem = Runtime.getRuntime().maxMemory();

        if (maxMem < .85 * min) {
            System.err.println("Heap limit is too low (" + (maxMem / (1024 * 1024)) +
                "MB), please increase heap size at least up to " + (min / (1024 * 1024)) + "MB.");

            System.exit(-1);
        }
    }
}
