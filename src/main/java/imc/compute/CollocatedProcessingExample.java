package imc.compute;

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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 *
 */
public class CollocatedProcessingExample {
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
            System.out.println();
            System.out.println(">>> Collocated processing example started.");

            // Assigning the affinity key to Canada country code (primary key).
            String affinityKey = "CAN";
            runCollocatedComputation(ignite, affinityKey);

            // Assigning the affinity key to India country code (primary key).
            affinityKey = "IND";
            runCollocatedComputation(ignite, affinityKey);
        }
    }

    private static void runCollocatedComputation(Ignite ignite, String affinityKey) {
        // Sending the logic to a cluster node that stores the affinity key.

        ignite.compute().affinityRun(Arrays.asList("Country", "City"), affinityKey,
            new IgniteRunnable() {

            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {
                System.out.println(">>>");

                // Getting an access to Countries.
                IgniteCache<String, BinaryObject> countries = ignite.cache("Country").withKeepBinary();

                System.out.println("Country: " + countries.get(affinityKey));

                IgniteCache<BinaryObject, BinaryObject> cities = ignite.cache("City").withKeepBinary();

                SqlFieldsQuery query = new SqlFieldsQuery("SELECT count(*) FROM City WHERE countryCode=?").
                    setArgs(affinityKey);

                //Safe since we use "affinityRun" method!
                query.setLocal(true);

                System.out.println("Number of cities: " + cities.query(query).getAll().get(0));

                System.out.println("");
            }
            });
    }
}
