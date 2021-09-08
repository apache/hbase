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
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    StoreContext ctx = StoreContext.getBuilder().
      withColumnFamilyDescriptor(fDescBuilder.build()).
      withRegionFileSystem(regionFs).
      build();
    return StoreFileTrackerFactory.create(conf, isPrimaryReplica, ctx);
  }

  public static Configuration mergeConfigurations(Configuration global,
    TableDescriptor table, ColumnFamilyDescriptor family) {
    return new CompoundConfiguration()
      .add(global)
      .addBytesMap(table.getValues())
      .addStringMap(family.getConfiguration())
      .addBytesMap(family.getValues());
  }
}
