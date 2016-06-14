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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsHeapMemoryManagerWrapper {

  /**
   * (the occupied size of the block cache in bytes) /
   * (the maximum amount of memory in bytes that can be used)
   */
  float getBlockCacheUsedPercent();

  /**
   * Returns the occupied size of the block cache, in bytes.
   */
  long getBlockCacheUsedSize();

  /**
   * (the global memstore size in bytes) /
   * (the maximum amount of memory in bytes that can be used)
   */
  float getMemStoreUsedPercent();

  /**
   * Return the global memstore size in bytes in the RegionServer
   */
  long getMemStoreUsedSize();
}
