/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

public interface MetricsHeapMemoryManagerWrapper {

  /**
   * Get ServerName
   */
  String getServerName();

  /**
   * Get the Cluster ID
   */
  String getClusterId();

  /**
   * Get Max heap size
   */
  long getMaxHeap();

  /**
   * Get Heap Used in percentage
   */
  float getHeapUsed();

  /**
   * Get Heap Used in size
   */
  long getHeapUsedSize();

  /**
   * Get BlockCache used in percentage
   */
  float getBlockCacheUsed();

  /**
   * Get BlockCached used in size
   */
  long getBlockCacheUsedSize();

  /**
   * Get MemStore used in percentage
   */
  float getMemStoreUsed();

  /**
   * Get MemStore used in size
   */
  long getMemStoreUsedSize();
}
