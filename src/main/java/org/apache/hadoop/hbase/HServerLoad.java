/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * This class encapsulates metrics for determining the load on a HRegionServer
 */
@ThriftStruct
public class HServerLoad implements WritableComparable<HServerLoad> {
  /** number of regions */
    // could just use regionLoad.size() but master.RegionManager likes to play
    // around with this value while passing HServerLoad objects around during
    // balancer calculations
  private int numberOfRegions;
  /** number of requests since last report */
  private int numberOfRequests;
  /** the amount of used heap, in MB */
  private int usedHeapMB;
  /** the maximum allowable size of the heap, in MB */
  private int maxHeapMB;
  /** per-region load metrics */
  private List<RegionLoad> regionLoad = new ArrayList<RegionLoad>();
  // lastLoadRefreshTime and expireAfter are only maintained by the 
  // master. They are not serialized and reported by the region servers
  public volatile /* transient */ long lastLoadRefreshTime = 0;
  public volatile /* transient */ long expireAfter = Long.MAX_VALUE;


  /**
   * Encapsulates per-region loading metrics.
   */
  @ThriftStruct
  public static class RegionLoad implements Writable {
    /** the region name */
    private byte[] name;
    /** the number of stores for the region */
    private int stores;
    /** the number of storefiles for the region */
    private int storefiles;
    /** the current total size of the store files for the region, in MB */
    private int storefileSizeMB;
    /** the current size of the memstore for the region, in MB */
    private int memstoreSizeMB;

    /**
     * The current total size of root-level store file indexes for the region,
     * in MB. The same as {@link #rootIndexSizeKB} but in MB.
     */
    private int storefileIndexSizeMB;

    /**
     * The current total size of root-level indexes for the region, in KB. Not
     * serialized because of backwards-compatibility issues.
     */
    private int rootIndexSizeKB = -1;

    /**
     * The total size of all index blocks, not just the root level, in KB. Not
     * serialized because of backwards-compatibility issues.
     */
    private int totalStaticIndexSizeKB = -1;

    /**
     * The total size of all Bloom filter blocks, not just loaded into the
     * block cache, in KB. Not serialized because of backwards-compatibility
     * issues.
     */
    private int totalStaticBloomSizeKB = -1;

    /**
     * Constructor, for Writable
     */
    public RegionLoad() {
        super();
    }

    /**
     * @param name
     * @param stores
     * @param storefiles
     * @param storefileSizeMB
     * @param memstoreSizeMB
     * @param storefileIndexSizeMB
     * @param totalReadRequest
     * @param totalReadRequest
     * @param readRequestPerSec
     * @param readRequestPerSec
     */
    @ThriftConstructor
    public RegionLoad(
        @ThriftField(1) final byte[] name,
        @ThriftField(2) final int stores,
        @ThriftField(3) final int storefiles,
        @ThriftField(4) final int storefileSizeMB,
        @ThriftField(5) final int memstoreSizeMB,
        @ThriftField(6) final int storefileIndexSizeMB,
        @ThriftField(7) final int rootIndexSizeKB,
        @ThriftField(8) final int totalStaticIndexSizeKB,
        @ThriftField(9) final int totalStaticBloomSizeKB) {
      this.name = name;
      this.stores = stores;
      this.storefiles = storefiles;
      this.storefileSizeMB = storefileSizeMB;
      this.memstoreSizeMB = memstoreSizeMB;
      this.storefileIndexSizeMB = storefileIndexSizeMB;
      this.rootIndexSizeKB = rootIndexSizeKB;
      this.totalStaticIndexSizeKB = totalStaticIndexSizeKB;
      this.totalStaticBloomSizeKB = totalStaticBloomSizeKB;
    }

    // Getters

    /**
     * @return the region name
     */
    @ThriftField(1)
    public byte[] getName() {
      return name;
    }

    /**
     * @return the region name as a string
     */
    public String getNameAsString() {
      return Bytes.toString(name);
    }

    /**
     * @return the number of stores
     */
    @ThriftField(2)
    public int getStores() {
      return stores;
    }

    /**
     * @return the number of storefiles
     */
    @ThriftField(3)
    public int getStorefiles() {
      return storefiles;
    }

    /**
     * @return the total size of the storefiles, in MB
     */
    @ThriftField(4)
    public int getStorefileSizeMB() {
      return storefileSizeMB;
    }

    /**
     * @return the memstore size, in MB
     */
    @ThriftField(5)
    public int getMemstoreSizeMB() {
      return memstoreSizeMB;
    }

    /**
     * @return the approximate size of storefile indexes on the heap, in MB
     */
    @ThriftField(6)
    public int getStorefileIndexSizeMB() {
      return storefileIndexSizeMB;
    }

    @ThriftField(7)
    public int getRootIndexSizeKB() {
      return rootIndexSizeKB;
    }

    @ThriftField(8)
    public int getTotalStaticIndexSizeKB() {
      return totalStaticIndexSizeKB;
    }

    @ThriftField(9)
    public int getTotalStaticBloomSizeKB() {
      return totalStaticBloomSizeKB;
    }

    // Setters

    /**
     * @param name the region name
     */
    public void setName(byte[] name) {
      this.name = name;
    }

    /**
     * @param stores the number of stores
     */
    public void setStores(int stores) {
      this.stores = stores;
    }

    /**
     * @param storefiles the number of storefiles
     */
    public void setStorefiles(int storefiles) {
      this.storefiles = storefiles;
    }

    /**
     * @param memstoreSizeMB the memstore size, in MB
     */
    public void setMemStoreSizeMB(int memstoreSizeMB) {
      this.memstoreSizeMB = memstoreSizeMB;
    }

    /**
     * @param storefileIndexSizeMB the approximate size of storefile indexes
     *  on the heap, in MB
     */
    public void setStorefileIndexSizeMB(int storefileIndexSizeMB) {
      this.storefileIndexSizeMB = storefileIndexSizeMB;
    }

    // Writable
    public void readFields(DataInput in) throws IOException {
      int namelen = in.readInt();
      this.name = new byte[namelen];
      in.readFully(this.name);
      this.stores = in.readInt();
      this.storefiles = in.readInt();
      this.storefileSizeMB = in.readInt();
      this.memstoreSizeMB = in.readInt();
      this.storefileIndexSizeMB = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(name.length);
      out.write(name);
      out.writeInt(stores);
      out.writeInt(storefiles);
      out.writeInt(storefileSizeMB);
      out.writeInt(memstoreSizeMB);
      out.writeInt(storefileIndexSizeMB);
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "stores",
        Integer.valueOf(this.stores));
      sb = Strings.appendKeyValue(sb, "storefiles",
        Integer.valueOf(this.storefiles));
      sb = Strings.appendKeyValue(sb, "storefileSizeMB",
          Integer.valueOf(this.storefileSizeMB));
      sb = Strings.appendKeyValue(sb, "memstoreSizeMB",
        Integer.valueOf(this.memstoreSizeMB));
      sb = Strings.appendKeyValue(sb, "storefileIndexSizeMB",
        Integer.valueOf(this.storefileIndexSizeMB));

      if (rootIndexSizeKB != -1) {
        sb = Strings.appendKeyValue(sb, "rootIndexSizeKB",
            Integer.valueOf(this.rootIndexSizeKB));
      }

      if (totalStaticIndexSizeKB != -1) {
        sb = Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
            Integer.valueOf(this.totalStaticIndexSizeKB));
      }

      if (totalStaticBloomSizeKB != -1) {
        sb = Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
          Integer.valueOf(this.totalStaticBloomSizeKB));
      }
      return sb.toString();
    }
  }

  /*
   * TODO: Other metrics that might be considered when the master is actually
   * doing load balancing instead of merely trying to decide where to assign
   * a region:
   * <ul>
   *   <li># of CPUs, heap size (to determine the "class" of machine). For
   *       now, we consider them to be homogeneous.</li>
   *   <li>#requests per region (Map<{String|HRegionInfo}, Integer>)</li>
   *   <li>#compactions and/or #splits (churn)</li>
   *   <li>server death rate (maybe there is something wrong with this server)</li>
   * </ul>
   */

  /** default constructor (used by Writable) */
  public HServerLoad() {
    super();
  }

  /**
   * Constructor
   * @param numberOfRequests
   * @param usedHeapMB
   * @param maxHeapMB
   */
  public HServerLoad(final int numberOfRequests, final int usedHeapMB,
      final int maxHeapMB) {
    this.numberOfRequests = numberOfRequests;
    this.usedHeapMB = usedHeapMB;
    this.maxHeapMB = maxHeapMB;
  }

  /**
   * Constructor
   * @param hsl the template HServerLoad
   */
  public HServerLoad(final HServerLoad hsl) {
    this(hsl.numberOfRequests, hsl.usedHeapMB, hsl.maxHeapMB);
    this.regionLoad.addAll(hsl.regionLoad);
  }

  @ThriftConstructor
  public HServerLoad(
      @ThriftField(1) int numberOfRegions,
      @ThriftField(2) int numberOfRequests,
      @ThriftField(3) int usedHeapMB,
      @ThriftField(4) int maxHeapMB,
      @ThriftField(5) List<RegionLoad> regionLoad,
      @ThriftField(6) long lastLoadRefreshTime,
      @ThriftField(7) long expireAfter) {
    this.numberOfRegions = numberOfRegions;
    this.numberOfRequests = numberOfRequests;
    this.usedHeapMB = usedHeapMB;
    this.maxHeapMB = maxHeapMB;
    this.regionLoad = regionLoad;
    this.lastLoadRefreshTime = lastLoadRefreshTime;
    this.expireAfter = expireAfter;
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
    // int load = numberOfRequests == 0 ? 1 : numberOfRequests;
    // load *= numberOfRegions == 0 ? 1 : numberOfRegions;
    // return load;
    return numberOfRegions;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return toString(1);
  }

  /**
   * Returns toString() with the number of requests divided by the message
   * interval in seconds
   * @param msgInterval
   * @return The load as a String
   */
  public String toString(int msgInterval) {
    StringBuilder sb = new StringBuilder();
    sb = Strings.appendKeyValue(sb, "requests",
      Integer.valueOf(numberOfRequests/msgInterval));
    sb = Strings.appendKeyValue(sb, "regions",
      Integer.valueOf(numberOfRegions));
    sb = Strings.appendKeyValue(sb, "usedHeap",
      Integer.valueOf(this.usedHeapMB));
    sb = Strings.appendKeyValue(sb, "maxHeap", Integer.valueOf(maxHeapMB));
    return sb.toString();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    return compareTo((HServerLoad)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = Integer.valueOf(numberOfRequests).hashCode();
    result ^= Integer.valueOf(numberOfRegions).hashCode();
    return result;
  }

  // Getters

  /**
   * @return the numberOfRegions
   */
  @ThriftField(1)
  public int getNumberOfRegions() {
    return numberOfRegions;
  }

  /**
   * @return the numberOfRequests
   */
  @ThriftField(2)
  public int getNumberOfRequests() {
    return numberOfRequests;
  }

  /**
   * @return the amount of heap in use, in MB
   */
  @ThriftField(3)
  public int getUsedHeapMB() {
    return usedHeapMB;
  }

  /**
   * @return the maximum allowable heap size, in MB
   */
  @ThriftField(4)
  public int getMaxHeapMB() {
    return maxHeapMB;
  }

  /**
   * @return region load metrics
   */
  public Collection<RegionLoad> getRegionsLoad() {
    return Collections.unmodifiableCollection(regionLoad);
  }

  /**
   * @return Count of storefiles on this regionserver
   */
  public int getStorefiles() {
    int count = 0;
    for (RegionLoad info: regionLoad)
      count += info.getStorefiles();
    return count;
  }

  /**
   * @return Total size of store files in MB
   */
  public int getStorefileSizeInMB() {
    int count = 0;
    for (RegionLoad info: regionLoad)
      count += info.getStorefileSizeMB();
    return count;
  }

  /**
   * @return Size of memstores in MB
   */
  public int getMemStoreSizeInMB() {
    int count = 0;
    for (RegionLoad info: regionLoad)
      count += info.getMemstoreSizeMB();
    return count;
  }

  /**
   * @return Size of store file indexes in MB
   */
  public int getStorefileIndexSizeInMB() {
    int count = 0;
    for (RegionLoad info: regionLoad)
      count += info.getStorefileIndexSizeMB();
    return count;
  }

  @ThriftField(5)
  public List<RegionLoad> getRegionLoad() {
    return regionLoad;
  }

  @ThriftField(6)
  public long getLastLoadRefreshTime() {
    return lastLoadRefreshTime;
  }

  @ThriftField(7)
  public long getExpireAfter() {
    return expireAfter;
  }



  // Setters


  /**
   * @param numberOfRegions the number of regions
   */
  public void setNumberOfRegions(int numberOfRegions) {
    this.numberOfRegions = numberOfRegions;
  }

  /**
   * @param numberOfRequests the number of requests to set
   */
  public void setNumberOfRequests(int numberOfRequests) {
    this.numberOfRequests = numberOfRequests;
  }

  /**
   * @param usedHeapMB the amount of heap in use, in MB
   */
  public void setUsedHeapMB(int usedHeapMB) {
    this.usedHeapMB = usedHeapMB;
  }

  /**
   * @param maxHeapMB the maximum allowable heap size, in MB
   */
  public void setMaxHeapMB(int maxHeapMB) {
    this.maxHeapMB = maxHeapMB;
  }

  /**
   * @param load Instance of HServerLoad
   */
  public void addRegionInfo(final HServerLoad.RegionLoad load) {
    this.numberOfRegions++;
    this.regionLoad.add(load);
  }

  /**
   * @param name
   * @param stores
   * @param storefiles
   * @param memstoreSizeMB
   * @param storefileIndexSizeMB
   * @deprecated Use {@link #addRegionInfo(RegionLoad)}
   */
  @Deprecated
  public void addRegionInfo(final byte[] name, final int stores,
      final int storefiles, final int storefileSizeMB,
      final int memstoreSizeMB, final int storefileIndexSizeMB,
      final int rootIndexSizeKB, final int totalStaticIndexSizeKB,
      final int totalStaticBloomSizeKB) {
    this.regionLoad.add(new HServerLoad.RegionLoad(name, stores, storefiles,
      storefileSizeMB, memstoreSizeMB, storefileIndexSizeMB, rootIndexSizeKB,
      totalStaticIndexSizeKB, totalStaticBloomSizeKB));
  }

  // Writable

  public void readFields(DataInput in) throws IOException {
    numberOfRequests = in.readInt();
    usedHeapMB = in.readInt();
    maxHeapMB = in.readInt();
    numberOfRegions = in.readInt();
    for (int i = 0; i < numberOfRegions; i++) {
      RegionLoad rl = new RegionLoad();
      rl.readFields(in);
      regionLoad.add(rl);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(numberOfRequests);
    out.writeInt(usedHeapMB);
    out.writeInt(maxHeapMB);
    out.writeInt(numberOfRegions);
    for (int i = 0; i < numberOfRegions; i++)
      regionLoad.get(i).write(out);
  }

  // Comparable

  public int compareTo(HServerLoad o) {
    return this.getLoad() - o.getLoad();
  }

}
