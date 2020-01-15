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

package imc.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * City key class to properly work with city objects using key-value and compute APIs.
 * No need to implement hashCode or equals. Ignite does this internally on top of the serialized data (BinaryObject).
 */
public class CityKey {
    /** */
    private int ID;

    /** */
    @AffinityKeyMapped
    private String COUNTRYCODE;

    /**
     * Constructor.
     *
     * @param id City ID.
     * @param countryCode Country code (affinity key).
     */
    public CityKey(int id, String countryCode) {
        this.ID = id;
        this.COUNTRYCODE = countryCode;
    }
}
