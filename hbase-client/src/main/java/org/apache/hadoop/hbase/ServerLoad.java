/**
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * This class is used for exporting current state of load on a RegionServer.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServerLoad {
  private int stores = 0;
  private int storefiles = 0;
  private int storeUncompressedSizeMB = 0;
  private int storefileSizeMB = 0;
  private int memstoreSizeMB = 0;
  private int storefileIndexSizeMB = 0;
  private int readRequestsCount = 0;
  private int writeRequestsCount = 0;
  private int rootIndexSizeKB = 0;
  private int totalStaticIndexSizeKB = 0;
  private int totalStaticBloomSizeKB = 0;
  private long totalCompactingKVs = 0;
  private long currentCompactedKVs = 0;
  
  public ServerLoad(ClusterStatusProtos.ServerLoad serverLoad) {
    this.serverLoad = serverLoad;
    for (ClusterStatusProtos.RegionLoad rl: serverLoad.getRegionLoadsList()) {
      stores += rl.getStores();
      storefiles += rl.getStorefiles();
      storeUncompressedSizeMB += rl.getStoreUncompressedSizeMB();
      storefileSizeMB += rl.getStorefileSizeMB();
      memstoreSizeMB += rl.getMemstoreSizeMB();
      storefileIndexSizeMB += rl.getStorefileIndexSizeMB();
      readRequestsCount += rl.getReadRequestsCount();
      writeRequestsCount += rl.getWriteRequestsCount();
      rootIndexSizeKB += rl.getRootIndexSizeKB();
      totalStaticIndexSizeKB += rl.getTotalStaticIndexSizeKB();
      totalStaticBloomSizeKB += rl.getTotalStaticBloomSizeKB();
      totalCompactingKVs += rl.getTotalCompactingKVs();
      currentCompactedKVs += rl.getCurrentCompactedKVs();
    }
    
  }

  // NOTE: Function name cannot start with "get" because then an OpenDataException is thrown because
  // HBaseProtos.ServerLoad cannot be converted to an open data type(see HBASE-5967).
  /* @return the underlying ServerLoad protobuf object */
  public ClusterStatusProtos.ServerLoad obtainServerLoadPB() {
    return serverLoad;
  }

  protected ClusterStatusProtos.ServerLoad serverLoad;

  /* @return number of requests  since last report. */
  public int getNumberOfRequests() {
    return serverLoad.getNumberOfRequests();
  }
  public boolean hasNumberOfRequests() {
    return serverLoad.hasNumberOfRequests();
  }

  /* @return total Number of requests from the start of the region server. */
  public int getTotalNumberOfRequests() {
    return serverLoad.getTotalNumberOfRequests();
  }
  public boolean hasTotalNumberOfRequests() {
    return serverLoad.hasTotalNumberOfRequests();
  }

  /* @return the amount of used heap, in MB. */
  public int getUsedHeapMB() {
    return serverLoad.getUsedHeapMB();
  }
  public boolean hasUsedHeapMB() {
    return serverLoad.hasUsedHeapMB();
  }

  /* @return the maximum allowable size of the heap, in MB. */
  public int getMaxHeapMB() {
    return serverLoad.getMaxHeapMB();
  }
  public boolean hasMaxHeapMB() {
    return serverLoad.hasMaxHeapMB();
  }

  public int getStores() {
    return stores;
  }

  public int getStorefiles() {
    return storefiles;
  }

  public int getStoreUncompressedSizeMB() {
    return storeUncompressedSizeMB;
  }

  public int getStorefileSizeInMB() {
    return storefileSizeMB;
  }

  public int getMemstoreSizeInMB() {
    return memstoreSizeMB;
  }

  public int getStorefileIndexSizeInMB() {
    return storefileIndexSizeMB;
  }

  public int getReadRequestsCount() {
    return readRequestsCount;
  }

  public int getWriteRequestsCount() {
    return writeRequestsCount;
  }

  public int getRootIndexSizeKB() {
    return rootIndexSizeKB;
  }

  public int getTotalStaticIndexSizeKB() {
    return totalStaticIndexSizeKB;
  }

  public int getTotalStaticBloomSizeKB() {
    return totalStaticBloomSizeKB;
  }

  public long getTotalCompactingKVs() {
    return totalCompactingKVs;
  }

  public long getCurrentCompactedKVs() {
    return currentCompactedKVs;
  }

  /**
   * @return the number of regions
   */
  public int getNumberOfRegions() {
    return serverLoad.getRegionLoadsCount();
  }

  public int getInfoServerPort() {
    return serverLoad.getInfoServerPort();
  }

  /**
   * Originally, this method factored in the effect of requests going to the
   * server as well. However, this does not interact very well with the current
   * region rebalancing code, which only factors number of regions. For the
   * interim, until we can figure out how to make rebalancing use all the info
   * available, we're just going to make load purely the number of regions.
   *
   * @return load factor for this server
   */
  public int getLoad() {
    // See above comment
    // int load = numberOfRequests == 0 ? 1 : numberOfRequests;
    // load *= numberOfRegions == 0 ? 1 : numberOfRegions;
    // return load;
    return getNumberOfRegions();
  }

  /**
   * @return region load metrics
   */
  public Map<byte[], RegionLoad> getRegionsLoad() {
    Map<byte[], RegionLoad> regionLoads =
      new TreeMap<byte[], RegionLoad>(Bytes.BYTES_COMPARATOR);
    for (ClusterStatusProtos.RegionLoad rl : serverLoad.getRegionLoadsList()) {
      RegionLoad regionLoad = new RegionLoad(rl);
      regionLoads.put(regionLoad.getName(), regionLoad);
    }
    return regionLoads;
  }

  /**
   * Return the RegionServer-level coprocessors
   * @return string array of loaded RegionServer-level coprocessors
   */
  public String[] getRegionServerCoprocessors() {
    List<Coprocessor> list = obtainServerLoadPB().getCoprocessorsList();
    String [] ret = new String[list.size()];
    int i = 0;
    for (Coprocessor elem : list) {
      ret[i++] = elem.getName();
    }

    return ret;
  }

  /**
   * Return the RegionServer-level and Region-level coprocessors
   * @return string array of loaded RegionServer-level and
   *         Region-level coprocessors
   */
  public String[] getRsCoprocessors() {
    // Need a set to remove duplicates, but since generated Coprocessor class
    // is not Comparable, make it a Set<String> instead of Set<Coprocessor>
    TreeSet<String> coprocessSet = new TreeSet<String>();
    for (Coprocessor coprocessor : obtainServerLoadPB().getCoprocessorsList()) {
      coprocessSet.add(coprocessor.getName());
    }
    return coprocessSet.toArray(new String[coprocessSet.size()]);
  }

  /**
   * @return number of requests per second received since the last report
   */
  public double getRequestsPerSecond() {
    return getNumberOfRequests();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
     StringBuilder sb =
        Strings.appendKeyValue(new StringBuilder(), "requestsPerSecond",
          Double.valueOf(getRequestsPerSecond()));
    Strings.appendKeyValue(sb, "numberOfOnlineRegions", Integer.valueOf(getNumberOfRegions()));
    sb = Strings.appendKeyValue(sb, "usedHeapMB", Integer.valueOf(this.getUsedHeapMB()));
    sb = Strings.appendKeyValue(sb, "maxHeapMB", Integer.valueOf(getMaxHeapMB()));
    sb = Strings.appendKeyValue(sb, "numberOfStores", Integer.valueOf(this.stores));
    sb = Strings.appendKeyValue(sb, "numberOfStorefiles", Integer.valueOf(this.storefiles));
    sb =
        Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
          Integer.valueOf(this.storeUncompressedSizeMB));
    sb = Strings.appendKeyValue(sb, "storefileSizeMB", Integer.valueOf(this.storefileSizeMB));
    if (this.storeUncompressedSizeMB != 0) {
      sb =
          Strings.appendKeyValue(
            sb,
            "compressionRatio",
            String.format("%.4f", (float) this.storefileSizeMB
                / (float) this.storeUncompressedSizeMB));
    }
    sb = Strings.appendKeyValue(sb, "memstoreSizeMB", Integer.valueOf(this.memstoreSizeMB));
    sb =
        Strings.appendKeyValue(sb, "storefileIndexSizeMB",
          Integer.valueOf(this.storefileIndexSizeMB));
    sb = Strings.appendKeyValue(sb, "readRequestsCount", Long.valueOf(this.readRequestsCount));
    sb = Strings.appendKeyValue(sb, "writeRequestsCount", Long.valueOf(this.writeRequestsCount));
    sb = Strings.appendKeyValue(sb, "rootIndexSizeKB", Integer.valueOf(this.rootIndexSizeKB));
    sb =
        Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
          Integer.valueOf(this.totalStaticIndexSizeKB));
    sb =
        Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
          Integer.valueOf(this.totalStaticBloomSizeKB));
    sb = Strings.appendKeyValue(sb, "totalCompactingKVs", Long.valueOf(this.totalCompactingKVs));
    sb = Strings.appendKeyValue(sb, "currentCompactedKVs", Long.valueOf(this.currentCompactedKVs));
    float compactionProgressPct = Float.NaN;
    if (this.totalCompactingKVs > 0) {
      compactionProgressPct =
          Float.valueOf((float) this.currentCompactedKVs / this.totalCompactingKVs);
    }
    sb = Strings.appendKeyValue(sb, "compactionProgressPct", compactionProgressPct);

    String[] coprocessorStrings = getRsCoprocessors();
    if (coprocessorStrings != null) {
      sb = Strings.appendKeyValue(sb, "coprocessors", Arrays.toString(coprocessorStrings));
    }
    return sb.toString();
  }

  public static final ServerLoad EMPTY_SERVERLOAD =
    new ServerLoad(ClusterStatusProtos.ServerLoad.newBuilder().build());
}
