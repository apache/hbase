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

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionLoad;
import org.apache.hadoop.hbase.util.Strings;

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
  
  public ServerLoad(HBaseProtos.ServerLoad serverLoad) {
    this.serverLoad = serverLoad;
    for (HBaseProtos.RegionLoad rl: serverLoad.getRegionLoadsList()) {
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

  /* @return the underlying ServerLoad protobuf object */
  public HBaseProtos.ServerLoad getServerLoadPB() {
    return serverLoad;
  }

  protected HBaseProtos.ServerLoad serverLoad;

  /* @return number of requests per second since last report. */
  public int getRequestsPerSecond() {
    return serverLoad.getRequestsPerSecond();
  }
  public boolean hasRequestsPerSecond() {
    return serverLoad.hasRequestsPerSecond();
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

  /* Returns list of RegionLoads, which contain information on the load of individual regions. */
  public List<RegionLoad> getRegionLoadsList() {
    return serverLoad.getRegionLoadsList();
  }
  public RegionLoad getRegionLoads(int index) {
    return serverLoad.getRegionLoads(index);
  }
  public int getRegionLoadsCount() {
    return serverLoad.getRegionLoadsCount();
  }

  /**
   * @return the list Regionserver-level coprocessors, e.g., WALObserver implementations.
   * Region-level coprocessors, on the other hand, are stored inside the RegionLoad objects.
   */
  public List<Coprocessor> getCoprocessorsList() {
    return serverLoad.getCoprocessorsList();
  }
  public Coprocessor getCoprocessors(int index) {
    return serverLoad.getCoprocessors(index);
  }
  public int getCoprocessorsCount() {
    return serverLoad.getCoprocessorsCount();
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

  public int getStorefileSizeMB() {
    return storefileSizeMB;
  }

  public int getMemstoreSizeMB() {
    return memstoreSizeMB;
  }

  public int getStorefileIndexSizeMB() {
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
   * Return the RegionServer-level coprocessors from a ServerLoad pb.
   * @param sl - ServerLoad
   * @return string array of loaded RegionServer-level coprocessors
   */
  public static String[] getRegionServerCoprocessors(ServerLoad sl) {
    if (sl == null) {
      return null;
    }

    List<Coprocessor> list = sl.getCoprocessorsList();
    String [] ret = new String[list.size()];
    int i = 0;
    for (Coprocessor elem : list) {
      ret[i++] = elem.getName();
    }

    return ret;
  }

  /**
   * Return the RegionServer-level and Region-level coprocessors
   * from a ServerLoad pb.
   * @param sl - ServerLoad
   * @return string array of loaded RegionServer-level and
   *         Region-level coprocessors
   */
  public static String[] getAllCoprocessors(ServerLoad sl) {
    if (sl == null) {
      return null;
    }

    // Need a set to remove duplicates, but since generated Coprocessor class
    // is not Comparable, make it a Set<String> instead of Set<Coprocessor>
    TreeSet<String> coprocessSet = new TreeSet<String>();
    for (Coprocessor coprocessor : sl.getCoprocessorsList()) {
      coprocessSet.add(coprocessor.getName());
    }
    for (RegionLoad rl : sl.getRegionLoadsList()) {
      for (Coprocessor coprocessor : rl.getCoprocessorsList()) {
        coprocessSet.add(coprocessor.getName());
      }
    }

    return coprocessSet.toArray(new String[0]);
  }


  @Override
  public String toString() {
    StringBuilder sb =
        Strings.appendKeyValue(new StringBuilder(), "requestsPerSecond",
          Integer.valueOf(this.getRequestsPerSecond()));
    Strings.appendKeyValue(sb, "numberOfOnlineRegions", Integer.valueOf(getRegionLoadsCount()));
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

    String[] coprocessorStrings = getAllCoprocessors(this);
    if (coprocessorStrings != null) {
      sb = Strings.appendKeyValue(sb, "coprocessors", Arrays.toString(coprocessorStrings));
    }
    return sb.toString();
  }

  public static final ServerLoad EMPTY_SERVERLOAD =
    new ServerLoad(HBaseProtos.ServerLoad.newBuilder().build());
}
