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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

/**
 * This example demonstrates very basic operations on cache, such as 'put' and 'get'.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} config/ignite-configuration.xml'}.
 * <p>
 * Alternatively you can run {@link NodeStartup} in another JVM which will
 * start node with {@code config/ignite-configuration.xml} configuration.
 */
public class PutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = PutGetExample.class.getSimpleName();

    /** Number of entries. **/
    private static final int KEY_CNT = 40;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        // Connecting in the client mode.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/ignite-configuration.xml")) {
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);

            System.out.println();
            System.out.println(">>> Cache put-get example started.");

            // Individual puts and gets.
            //putData(cache);

            // Bulk puts and gets.
            getData(cache);
        };
    }

    /**
     * Execute individual puts and gets.
     *
     * @throws IgniteException If failed.
     */
    private static void putData(IgniteCache<Integer, String> cache) throws IgniteException {
        // Store keys in cache.
        for (int i = 0; i < KEY_CNT; i++)
            cache.put(i, Integer.toString(i));

        System.out.println(">>> Stored values in cache.");
    }

    private static void getData(IgniteCache<Integer, String> cache) throws IgniteException {
        System.out.println(">>> Reading values from cache.");


        for (int i = 0; i < KEY_CNT; i++) {
            String val = cache.get(i);

            if (val == null) {
                System.err.println("ALARM - Data loss: key=" + i);
            }

            System.out.println("Got [key=" + i + ", val=" + val + ']');
        }
    }
}
