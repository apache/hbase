/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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
 * <p/>
 * The current implementations are:
 * <ul>
 * <li><em>default</em>: DefaultStoreFileTracker, see {@link DefaultStoreFileTracker}.</li>
 * <li><em>file</em>:FileBasedStoreFileTracker, see {@link FileBasedStoreFileTracker}.</li>
 * <li><em>migration</em>:MigrationStoreFileTracker, see {@link MigrationStoreFileTracker}.</li>
 * </ul>
 * @see DefaultStoreFileTracker
 * @see FileBasedStoreFileTracker
 * @see MigrationStoreFileTracker
 */
@InterfaceAudience.Private
public final class StoreFileTrackerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerFactory.class);

  public static final String TRACKER_IMPL = "hbase.store.file-tracker.impl";

  /**
   * Maps between configuration names for trackers and implementation classes.
   */
  public enum Trackers {
    DEFAULT(DefaultStoreFileTracker.class), FILE(FileBasedStoreFileTracker.class),
    MIGRATION(MigrationStoreFileTracker.class);

    final Class<? extends StoreFileTracker> clazz;

    Trackers(Class<? extends StoreFileTracker> clazz) {
      this.clazz = clazz;
    }
  }

  private static final Map<Class<? extends StoreFileTracker>, Trackers> CLASS_TO_ENUM = reverse();

  private static Map<Class<? extends StoreFileTracker>, Trackers> reverse() {
    Map<Class<? extends StoreFileTracker>, Trackers> map = new HashMap<>();
    for (Trackers tracker : Trackers.values()) {
      map.put(tracker.clazz, tracker);
    }
    return Collections.unmodifiableMap(map);
  }

  private StoreFileTrackerFactory() {
  }

  public static String getStoreFileTrackerName(Configuration conf) {
    return conf.get(TRACKER_IMPL, Trackers.DEFAULT.name());
  }

  static String getStoreFileTrackerName(Class<? extends StoreFileTracker> clazz) {
    Trackers name = CLASS_TO_ENUM.get(clazz);
    return name != null ? name.name() : clazz.getName();
  }

  private static Class<? extends StoreFileTracker> getTrackerClass(Configuration conf) {
    try {
      Trackers tracker = Trackers.valueOf(getStoreFileTrackerName(conf).toUpperCase());
      return tracker.clazz;
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      return conf.getClass(TRACKER_IMPL, Trackers.DEFAULT.clazz, StoreFileTracker.class);
    }
  }

  public static StoreFileTracker create(Configuration conf, boolean isPrimaryReplica,
    StoreContext ctx) {
    Class<? extends StoreFileTracker> tracker = getTrackerClass(conf);
    LOG.info("instantiating StoreFileTracker impl {}", tracker.getName());
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }

  /**
   * Used at master side when splitting/merging regions, as we do not have a Store, thus no
   * StoreContext at master side.
   */
  public static StoreFileTracker create(Configuration conf, boolean isPrimaryReplica, String family,
    HRegionFileSystem regionFs) {
    ColumnFamilyDescriptorBuilder fDescBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
    StoreContext ctx = StoreContext.getBuilder().withColumnFamilyDescriptor(fDescBuilder.build())
      .withRegionFileSystem(regionFs).build();
    return StoreFileTrackerFactory.create(conf, isPrimaryReplica, ctx);
  }

  public static Configuration mergeConfigurations(Configuration global, TableDescriptor table,
    ColumnFamilyDescriptor family) {
    return StoreUtils.createStoreConfiguration(global, table, family);
  }

  /**
   * Create store file tracker to be used as source or destination for
   * {@link MigrationStoreFileTracker}.
   */
  static StoreFileTrackerBase createForMigration(Configuration conf, String configName,
    boolean isPrimaryReplica, StoreContext ctx) {
    String trackerName =
      Preconditions.checkNotNull(conf.get(configName), "config %s is not set", configName);
    Class<? extends StoreFileTrackerBase> tracker;
    try {
      tracker =
        Trackers.valueOf(trackerName.toUpperCase()).clazz.asSubclass(StoreFileTrackerBase.class);
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      try {
        tracker = Class.forName(trackerName).asSubclass(StoreFileTrackerBase.class);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(cnfe);
      }
    }
    // prevent nest of MigrationStoreFileTracker, it will cause infinite recursion.
    if (MigrationStoreFileTracker.class.isAssignableFrom(tracker)) {
      throw new IllegalArgumentException("Should not specify " + configName + " as " +
        Trackers.MIGRATION + " because it can not be nested");
    }
    LOG.info("instantiating StoreFileTracker impl {} as {}", tracker.getName(), configName);
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }

  public static void persistTrackerConfig(Configuration conf, TableDescriptorBuilder builder) {
    TableDescriptor tableDescriptor = builder.build();
    ColumnFamilyDescriptor cfDesc = tableDescriptor.getColumnFamilies()[0];
    StoreContext context = StoreContext.getBuilder().withColumnFamilyDescriptor(cfDesc).build();
    StoreFileTracker tracker = StoreFileTrackerFactory.create(conf, true, context);
    tracker.persistConfiguration(builder);
  }
}
