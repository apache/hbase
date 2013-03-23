/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
/**
 * RegionContext gives a container for all the member variables in the
 * HRegion class which the RegionScanner needs in its constructor. Earlier,
 * since the RegionScanner was an inner class to HRegion, these members were
 * accessible. Now that the RegionScanner is an external class, these variables
 * are packaged into RegionContext.
 *
 * @author manukranthk
 *
 */
public class RegionContext {
  final private Map<byte[], Store> stores;
  final private ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;
  final private KeyValue.KVComparator comparator;
  final private MultiVersionConsistencyControl mvcc;
  final private AtomicBoolean closing;
  final private AtomicBoolean closed;
  final private HRegionInfo regionInfo;
  final private AtomicInteger rowReadCnt;

  public RegionContext(Map<byte[], Store> stores,
      ConcurrentHashMap<RegionScanner, Long> scannerReadPoints,
      KeyValue.KVComparator comparator,
      MultiVersionConsistencyControl mvcc,
      AtomicBoolean closing,
      AtomicBoolean closed,
      HRegionInfo regionInfo,
      AtomicInteger rowReadCnt) {
    this.stores = stores;
    this.scannerReadPoints = scannerReadPoints;
    this.comparator = comparator;
    this.mvcc = mvcc;
    this.closing = closing;
    this.closed = closed;
    this.regionInfo = regionInfo;
    this.rowReadCnt = rowReadCnt;
  }

  /*
   * Constructor to create a region context for
   * opening a read only RegionScanner
   */
  public RegionContext(Map<byte[], Store> stores, HRegionInfo regionInfo) {
    this.stores = stores;
    this.scannerReadPoints = new ConcurrentHashMap<RegionScanner, Long>();
    this.comparator = regionInfo.getComparator();
    this.mvcc = new MultiVersionConsistencyControl();
    this.mvcc.setThreadReadPoint(Long.MAX_VALUE);
    this.closing = new AtomicBoolean(false);
    this.closed = new AtomicBoolean(false);
    this.regionInfo = regionInfo;
    this.rowReadCnt = new AtomicInteger(0);
  }

  public Map<byte[], Store> getStores() {
    return this.stores;
  }

  public ConcurrentHashMap<RegionScanner, Long> getScannerReadPoints() {
    return this.scannerReadPoints;
  }

  public KeyValue.KVComparator getComparator() {
    return this.comparator;
  }

  public MultiVersionConsistencyControl getmvcc() {
    return this.mvcc;
  }

  public AtomicBoolean getClosing() {
    return this.closing;
  }

  public AtomicBoolean getClosed() {
    return this.closed;
  }

  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  public AtomicInteger getRowReadCnt() {
    return this.rowReadCnt;
  }
}
