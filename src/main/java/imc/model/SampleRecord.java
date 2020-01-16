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

import imc.compute.ComputeVsDataPullingExample;
import java.sql.Date;

/**
 * Dummy class for {@link ComputeVsDataPullingExample}.
 */
public class SampleRecord {
    private long distance;

    private float temperature;

    private Date time;

    private byte[] otherData = new byte[1024];

    public SampleRecord(long distance, float temperature, Date time) {
        this.distance = distance;
        this.temperature = temperature;
        this.time = time;
    }

    public long getDistance() {
        return distance;
    }

    public float getTemperature() {
        return temperature;
    }

    public Date getTime() {
        return time;
    }

    @Override public String toString() {
        return "SampleRecord{" +
            "distance=" + distance +
            ", temperature=" + temperature +
            ", time=" + time +
            '}';
    }
}
