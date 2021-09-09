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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Factory method for creating store file tracker.
 */
@InterfaceAudience.Private
public final class StoreFileTrackerFactory {
  public static final String TRACK_IMPL = "hbase.store.file-tracker.impl";
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerFactory.class);

  public static StoreFileTracker create(Configuration conf, boolean isPrimaryReplica,
    StoreContext ctx) {
    Class<? extends StoreFileTracker> tracker =
      conf.getClass(TRACK_IMPL, DefaultStoreFileTracker.class, StoreFileTracker.class);
    LOG.info("instantiating StoreFileTracker impl {}", tracker.getName());
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }

  public static StoreFileTracker create(Configuration conf, boolean isPrimaryReplica, String family,
    HRegionFileSystem regionFs) {
    ColumnFamilyDescriptorBuilder fDescBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
    StoreContext ctx = StoreContext.getBuilder().withColumnFamilyDescriptor(fDescBuilder.build())
      .withRegionFileSystem(regionFs).build();
    return StoreFileTrackerFactory.create(conf, TRACK_IMPL, isPrimaryReplica, ctx);
  }

  public static Configuration mergeConfigurations(Configuration global, TableDescriptor table,
    ColumnFamilyDescriptor family) {
    return StoreUtils.createStoreConfiguration(global, table, family);
  }

  static StoreFileTrackerBase create(Configuration conf, String configName,
    boolean isPrimaryReplica, StoreContext ctx) {
    String className =
      Preconditions.checkNotNull(conf.get(configName), "config %s is not set", configName);
    Class<? extends StoreFileTrackerBase> tracker;
    try {
      tracker = Class.forName(className).asSubclass(StoreFileTrackerBase.class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    LOG.info("instantiating StoreFileTracker impl {} as {}", tracker.getName(), configName);
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }
}
