/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.backoff;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;

/**
 * Track the statistics for a single region
 */
@InterfaceAudience.Private
public class ServerStatistics {

  private Map<byte[], RegionStatistics>
      stats = new TreeMap<byte[], RegionStatistics>(Bytes.BYTES_COMPARATOR);

  /**
   * Good enough attempt. Last writer wins. It doesn't really matter which one gets to update,
   * as something gets set
   * @param region
   * @param currentStats
   */
  public void update(byte[] region, ClientProtos.RegionLoadStats currentStats) {
    RegionStatistics regionStat = this.stats.get(region);
    if(regionStat == null){
      regionStat = new RegionStatistics();
      this.stats.put(region, regionStat);
    }

    regionStat.update(currentStats);
  }

  @InterfaceAudience.Private
  public RegionStatistics getStatsForRegion(byte[] regionName){
    return stats.get(regionName);
  }

  public static class RegionStatistics {
    private int memstoreLoad = 0;
    private int heapOccupancy = 0;
    private int compactionPressure = 0;

    public void update(ClientProtos.RegionLoadStats currentStats) {
      this.memstoreLoad = currentStats.getMemstoreLoad();
      this.heapOccupancy = currentStats.getHeapOccupancy();
      this.compactionPressure = currentStats.getCompactionPressure();
    }

    public int getMemstoreLoadPercent(){
      return this.memstoreLoad;
    }

    public int getHeapOccupancyPercent(){
      return this.heapOccupancy;
    }

    public int getCompactionPressure() {
      return compactionPressure;
    }

  }
}
