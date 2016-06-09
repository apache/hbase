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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.ResizableBlockCache;

@InterfaceAudience.Private
public class MetricsHeapMemoryManagerWrapperImpl
    implements MetricsHeapMemoryManagerWrapper {

  private MemoryUsage memUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();

  private final Server server;
  private final RegionServerAccounting rsAccounting;
  private final ResizableBlockCache blockCache;

  public MetricsHeapMemoryManagerWrapperImpl(final Server server,
      final RegionServerAccounting rsAccounting, ResizableBlockCache blockCache) {
    this.server = server;
    this.rsAccounting = rsAccounting;
    this.blockCache = blockCache;
  }

  @Override
  public String getServerName() {
    ServerName serverName = server.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public long getMaxHeap() {
    return memUsage.getMax();
  }

  @Override
  public float getHeapUsed() {
    return (float) memUsage.getUsed() / (float) memUsage.getCommitted();
  }

  @Override
  public long getHeapUsedSize() {
    return memUsage.getUsed();
  }

  @Override
  public float getBlockCacheUsed() {
    return (float) getBlockCacheUsedSize() / (float) memUsage.getMax();
  }

  @Override
  public long getBlockCacheUsedSize() {
    return blockCache.getCurrentSize();
  }

  @Override
  public float getMemStoreUsed() {
    return (float) getMemStoreUsedSize() / (float) memUsage.getMax();
  }

  @Override
  public long getMemStoreUsedSize() {
    return rsAccounting.getGlobalMemstoreSize();
  }
}
