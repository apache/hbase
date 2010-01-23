/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

/**
 * An MBean exposing various region ops and info.
 */
public interface IdxRegionMBean {

  /**
   * Checks whether the region being exposed by this MBean is still alive.
   *
   * @return whether the region being exposed by this MBean is still alive.
   */
  boolean isValid();

  /**
   * The number of keys in the index which is equivalent to the number of top
   * level rows in this region.
   *
   * @return the number of keys in the index.
   */
  int getNumberOfIndexedKeys();

  /**
   * The total heap size, in bytes, used by the indexes and their overhead.
   *
   * @return the total index heap size in bytes.
   */
  long getIndexesTotalHeapSize();

  /**
   * Gets the total number of indexed scan since the last reset.
   *
   * @return the total number of indexed scans.
   */
  public long getTotalIndexedScans();

  /**
   * Resets the total number of indexed scans.
   *
   * @return the number of indexed scans just before the reset
   */
  public long resetTotalIndexedScans();

  /**
   * Gets the total number of non indexed scan since the last reset.
   *
   * @return the total number of indexed scans.
   */
  public long getTotalNonIndexedScans();

  /**
   * Resets the total number of non indexed scans.
   *
   * @return the number of indexed scans just before the reset
   */
  public long resetTotalNonIndexedScans();

  /**
   * Exposes the number of indexed scans currently ongoing in the system.
   *
   * @return the number of ongoing indexed scans
   */
  public long getNumberOfOngoingIndexedScans();

  /**
   * Gets the index build times buffer as a comma delimited string
   * where each item is the milliseconds required to rebuild the indexes.
   *
   * @return a rolling buffer of index build times
   */
  public String getIndexBuildTimes();

  /**
   * Resets the index build times buffer.
   *
   * @return the previous build times buffer
   */
  public String resetIndexBuildTimes();

}
