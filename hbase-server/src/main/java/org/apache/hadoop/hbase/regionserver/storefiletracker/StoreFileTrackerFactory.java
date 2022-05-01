/*
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
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
    DEFAULT(DefaultStoreFileTracker.class),
    FILE(FileBasedStoreFileTracker.class),
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

  public static String getStoreFileTrackerName(Class<? extends StoreFileTracker> clazz) {
    Trackers name = CLASS_TO_ENUM.get(clazz);
    return name != null ? name.name() : clazz.getName();
  }

  public static Class<? extends StoreFileTracker> getTrackerClass(Configuration conf) {
    try {
      Trackers tracker = Trackers.valueOf(getStoreFileTrackerName(conf).toUpperCase());
      return tracker.clazz;
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      return conf.getClass(TRACKER_IMPL, Trackers.DEFAULT.clazz, StoreFileTracker.class);
    }
  }

  public static Class<? extends StoreFileTracker> getTrackerClass(String trackerNameOrClass) {
    try {
      Trackers tracker = Trackers.valueOf(trackerNameOrClass.toUpperCase());
      return tracker.clazz;
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      try {
        return Class.forName(trackerNameOrClass).asSubclass(StoreFileTracker.class);
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
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
  public static StoreFileTracker create(Configuration conf, TableDescriptor td,
    ColumnFamilyDescriptor cfd, HRegionFileSystem regionFs) {
    StoreContext ctx =
      StoreContext.getBuilder().withColumnFamilyDescriptor(cfd).withRegionFileSystem(regionFs)
        .withFamilyStoreDirectoryPath(regionFs.getStoreDir(cfd.getNameAsString())).build();
    return StoreFileTrackerFactory.create(mergeConfigurations(conf, td, cfd), true, ctx);
  }

  private static Configuration mergeConfigurations(Configuration global, TableDescriptor table,
    ColumnFamilyDescriptor family) {
    return StoreUtils.createStoreConfiguration(global, table, family);
  }

  static Class<? extends StoreFileTrackerBase>
    getStoreFileTrackerClassForMigration(Configuration conf, String configName) {
    String trackerName =
      Preconditions.checkNotNull(conf.get(configName), "config %s is not set", configName);
    try {
      return Trackers.valueOf(trackerName.toUpperCase()).clazz
        .asSubclass(StoreFileTrackerBase.class);
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      try {
        return Class.forName(trackerName).asSubclass(StoreFileTrackerBase.class);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(cnfe);
      }
    }
  }

  /**
   * Create store file tracker to be used as source or destination for
   * {@link MigrationStoreFileTracker}.
   */
  static StoreFileTrackerBase createForMigration(Configuration conf, String configName,
    boolean isPrimaryReplica, StoreContext ctx) {
    Class<? extends StoreFileTrackerBase> tracker =
      getStoreFileTrackerClassForMigration(conf, configName);
    // prevent nest of MigrationStoreFileTracker, it will cause infinite recursion.
    if (MigrationStoreFileTracker.class.isAssignableFrom(tracker)) {
      throw new IllegalArgumentException("Should not specify " + configName + " as "
        + Trackers.MIGRATION + " because it can not be nested");
    }
    LOG.info("instantiating StoreFileTracker impl {} as {}", tracker.getName(), configName);
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }

  public static TableDescriptor updateWithTrackerConfigs(Configuration conf,
    TableDescriptor descriptor) {
    // CreateTableProcedure needs to instantiate the configured SFT impl, in order to update table
    // descriptors with the SFT impl specific configs. By the time this happens, the table has no
    // regions nor stores yet, so it can't create a proper StoreContext.
    if (StringUtils.isEmpty(descriptor.getValue(TRACKER_IMPL))) {
      StoreFileTracker tracker = StoreFileTrackerFactory.create(conf, true, null);
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(descriptor);
      return tracker.updateWithTrackerConfigs(builder).build();
    }
    return descriptor;
  }

  public static boolean isMigration(Class<?> clazz) {
    return MigrationStoreFileTracker.class.isAssignableFrom(clazz);
  }

  // should not use MigrationStoreFileTracker for new family
  private static void checkForNewFamily(Configuration conf, TableDescriptor table,
    ColumnFamilyDescriptor family) throws IOException {
    Configuration mergedConf = mergeConfigurations(conf, table, family);
    Class<? extends StoreFileTracker> tracker = getTrackerClass(mergedConf);
    if (MigrationStoreFileTracker.class.isAssignableFrom(tracker)) {
      throw new DoNotRetryIOException(
        "Should not use " + Trackers.MIGRATION + " as store file tracker for new family "
          + family.getNameAsString() + " of table " + table.getTableName());
    }
  }

  /**
   * Pre check when creating a new table.
   * <p/>
   * For now, only make sure that we do not use {@link Trackers#MIGRATION} for newly created tables.
   * @throws IOException when there are check errors, the upper layer should fail the
   *                     {@code CreateTableProcedure}.
   */
  public static void checkForCreateTable(Configuration conf, TableDescriptor table)
    throws IOException {
    for (ColumnFamilyDescriptor family : table.getColumnFamilies()) {
      checkForNewFamily(conf, table, family);
    }
  }

  /**
   * Pre check when modifying a table.
   * <p/>
   * The basic idea is when you want to change the store file tracker implementation, you should use
   * {@link Trackers#MIGRATION} first and then change to the destination store file tracker
   * implementation.
   * <p/>
   * There are several rules:
   * <ul>
   * <li>For newly added family, you should not use {@link Trackers#MIGRATION}.</li>
   * <li>For modifying a family:
   * <ul>
   * <li>If old tracker is {@link Trackers#MIGRATION}, then:
   * <ul>
   * <li>The new tracker is also {@link Trackers#MIGRATION}, then they must have the same src and
   * dst tracker.</li>
   * <li>The new tracker is not {@link Trackers#MIGRATION}, then the new tracker must be the dst
   * tracker of the old tracker.</li>
   * </ul>
   * </li>
   * <li>If the old tracker is not {@link Trackers#MIGRATION}, then:
   * <ul>
   * <li>If the new tracker is {@link Trackers#MIGRATION}, then the old tracker must be the src
   * tracker of the new tracker.</li>
   * <li>If the new tracker is not {@link Trackers#MIGRATION}, then the new tracker must be the same
   * with old tracker.</li>
   * </ul>
   * </li>
   * </ul>
   * </li>
   * </ul>
   * @throws IOException when there are check errors, the upper layer should fail the
   *                     {@code ModifyTableProcedure}.
   */
  public static void checkForModifyTable(Configuration conf, TableDescriptor oldTable,
    TableDescriptor newTable) throws IOException {
    for (ColumnFamilyDescriptor newFamily : newTable.getColumnFamilies()) {
      ColumnFamilyDescriptor oldFamily = oldTable.getColumnFamily(newFamily.getName());
      if (oldFamily == null) {
        checkForNewFamily(conf, newTable, newFamily);
        continue;
      }
      Configuration oldConf = mergeConfigurations(conf, oldTable, oldFamily);
      Configuration newConf = mergeConfigurations(conf, newTable, newFamily);

      Class<? extends StoreFileTracker> oldTracker = getTrackerClass(oldConf);
      Class<? extends StoreFileTracker> newTracker = getTrackerClass(newConf);

      if (MigrationStoreFileTracker.class.isAssignableFrom(oldTracker)) {
        Class<? extends StoreFileTracker> oldSrcTracker =
          MigrationStoreFileTracker.getSrcTrackerClass(oldConf);
        Class<? extends StoreFileTracker> oldDstTracker =
          MigrationStoreFileTracker.getDstTrackerClass(oldConf);
        if (oldTracker.equals(newTracker)) {
          // confirm that we have the same src tracker and dst tracker
          Class<? extends StoreFileTracker> newSrcTracker =
            MigrationStoreFileTracker.getSrcTrackerClass(newConf);
          if (!oldSrcTracker.equals(newSrcTracker)) {
            throw new DoNotRetryIOException(
              "The src tracker has been changed from " + getStoreFileTrackerName(oldSrcTracker)
                + " to " + getStoreFileTrackerName(newSrcTracker) + " for family "
                + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newDstTracker =
            MigrationStoreFileTracker.getDstTrackerClass(newConf);
          if (!oldDstTracker.equals(newDstTracker)) {
            throw new DoNotRetryIOException(
              "The dst tracker has been changed from " + getStoreFileTrackerName(oldDstTracker)
                + " to " + getStoreFileTrackerName(newDstTracker) + " for family "
                + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        } else {
          // we can only change to the dst tracker
          if (!newTracker.equals(oldDstTracker)) {
            throw new DoNotRetryIOException(
              "Should migrate tracker to " + getStoreFileTrackerName(oldDstTracker) + " but got "
                + getStoreFileTrackerName(newTracker) + " for family " + newFamily.getNameAsString()
                + " of table " + newTable.getTableName());
          }
        }
      } else {
        if (!oldTracker.equals(newTracker)) {
          // can only change to MigrationStoreFileTracker and the src tracker should be the old
          // tracker
          if (!MigrationStoreFileTracker.class.isAssignableFrom(newTracker)) {
            throw new DoNotRetryIOException("Should change to " + Trackers.MIGRATION
              + " first when migrating from " + getStoreFileTrackerName(oldTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newSrcTracker =
            MigrationStoreFileTracker.getSrcTrackerClass(newConf);
          if (!oldTracker.equals(newSrcTracker)) {
            throw new DoNotRetryIOException(
              "Should use src tracker " + getStoreFileTrackerName(oldTracker) + " first but got "
                + getStoreFileTrackerName(newSrcTracker) + " when migrating from "
                + getStoreFileTrackerName(oldTracker) + " for family " + newFamily.getNameAsString()
                + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newDstTracker =
            MigrationStoreFileTracker.getDstTrackerClass(newConf);
          // the src and dst tracker should not be the same
          if (newSrcTracker.equals(newDstTracker)) {
            throw new DoNotRetryIOException("The src tracker and dst tracker are both "
              + getStoreFileTrackerName(newSrcTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        }
      }
    }
  }

  /**
   * Makes sure restoring a snapshot does not break the current SFT setup follows
   * StoreUtils.createStoreConfiguration
   * @param currentTableDesc  Existing Table's TableDescriptor
   * @param snapshotTableDesc Snapshot's TableDescriptor
   * @param baseConf          Current global configuration
   * @throws RestoreSnapshotException if restore would break the current SFT setup
   */
  public static void validatePreRestoreSnapshot(TableDescriptor currentTableDesc,
    TableDescriptor snapshotTableDesc, Configuration baseConf) throws RestoreSnapshotException {

    for (ColumnFamilyDescriptor cfDesc : currentTableDesc.getColumnFamilies()) {
      ColumnFamilyDescriptor snapCFDesc = snapshotTableDesc.getColumnFamily(cfDesc.getName());
      // if there is no counterpart in the snapshot it will be just deleted so the config does
      // not matter
      if (snapCFDesc != null) {
        Configuration currentCompositeConf =
          StoreUtils.createStoreConfiguration(baseConf, currentTableDesc, cfDesc);
        Configuration snapCompositeConf =
          StoreUtils.createStoreConfiguration(baseConf, snapshotTableDesc, snapCFDesc);
        Class<? extends StoreFileTracker> currentSFT =
          StoreFileTrackerFactory.getTrackerClass(currentCompositeConf);
        Class<? extends StoreFileTracker> snapSFT =
          StoreFileTrackerFactory.getTrackerClass(snapCompositeConf);

        // restoration is not possible if there is an SFT mismatch
        if (currentSFT != snapSFT) {
          throw new RestoreSnapshotException(
            "Restoring Snapshot is not possible because " + " the config for column family "
              + cfDesc.getNameAsString() + " has incompatible configuration. Current SFT: "
              + currentSFT + " SFT from snapshot: " + snapSFT);
        }
      }
    }
  }
}
