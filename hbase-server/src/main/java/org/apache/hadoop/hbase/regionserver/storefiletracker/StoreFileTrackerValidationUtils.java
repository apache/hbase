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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.Trackers;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class StoreFileTrackerValidationUtils {

  private StoreFileTrackerValidationUtils() {
  }

  // should not use MigrationStoreFileTracker for new family
  private static void checkForNewFamily(Configuration conf, TableDescriptor table,
    ColumnFamilyDescriptor family) throws IOException {
    Configuration mergedConf = StoreUtils.createStoreConfiguration(conf, table, family);
    Class<? extends StoreFileTracker> tracker = StoreFileTrackerFactory.getTrackerClass(mergedConf);
    if (MigrationStoreFileTracker.class.isAssignableFrom(tracker)) {
      throw new DoNotRetryIOException(
        "Should not use " + Trackers.MIGRATION + " as store file tracker for new family " +
          family.getNameAsString() + " of table " + table.getTableName());
    }
  }

  /**
   * Pre check when creating a new table.
   * <p/>
   * For now, only make sure that we do not use {@link Trackers#MIGRATION} for newly created tables.
   * @throws IOException when there are check errors, the upper layer should fail the
   *           {@code CreateTableProcedure}.
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
   *           {@code ModifyTableProcedure}.
   */
  public static void checkForModifyTable(Configuration conf, TableDescriptor oldTable,
    TableDescriptor newTable, boolean isTableDisabled) throws IOException {
    for (ColumnFamilyDescriptor newFamily : newTable.getColumnFamilies()) {
      ColumnFamilyDescriptor oldFamily = oldTable.getColumnFamily(newFamily.getName());
      if (oldFamily == null) {
        checkForNewFamily(conf, newTable, newFamily);
        continue;
      }
      Configuration oldConf = StoreUtils.createStoreConfiguration(conf, oldTable, oldFamily);
      Configuration newConf = StoreUtils.createStoreConfiguration(conf, newTable, newFamily);

      Class<? extends StoreFileTracker> oldTracker =
        StoreFileTrackerFactory.getTrackerClass(oldConf);
      Class<? extends StoreFileTracker> newTracker =
        StoreFileTrackerFactory.getTrackerClass(newConf);

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
            throw new DoNotRetryIOException("The src tracker has been changed from " +
              StoreFileTrackerFactory.getStoreFileTrackerName(oldSrcTracker) + " to " +
              StoreFileTrackerFactory.getStoreFileTrackerName(newSrcTracker) + " for family " +
              newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newDstTracker =
            MigrationStoreFileTracker.getDstTrackerClass(newConf);
          if (!oldDstTracker.equals(newDstTracker)) {
            throw new DoNotRetryIOException("The dst tracker has been changed from " +
              StoreFileTrackerFactory.getStoreFileTrackerName(oldDstTracker) + " to " +
              StoreFileTrackerFactory.getStoreFileTrackerName(newDstTracker) + " for family " +
              newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        } else {
          // do not allow changing from MIGRATION to its dst SFT implementation while the table is
          // disabled. We need to open the HRegion to migrate the tracking information while the SFT
          // implementation is MIGRATION, otherwise we may loss data. See HBASE-26611 for more
          // details.
          if (isTableDisabled) {
            throw new TableNotEnabledException(
              "Should not change store file tracker implementation from " +
                StoreFileTrackerFactory.Trackers.MIGRATION.name() + " while table " +
                newTable.getTableName() + " is disabled");
          }
          // we can only change to the dst tracker
          if (!newTracker.equals(oldDstTracker)) {
            throw new DoNotRetryIOException("Should migrate tracker to " +
              StoreFileTrackerFactory.getStoreFileTrackerName(oldDstTracker) + " but got " +
              StoreFileTrackerFactory.getStoreFileTrackerName(newTracker) + " for family " +
              newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        }
      } else {
        if (!oldTracker.equals(newTracker)) {
          // can only change to MigrationStoreFileTracker and the src tracker should be the old
          // tracker
          if (!MigrationStoreFileTracker.class.isAssignableFrom(newTracker)) {
            throw new DoNotRetryIOException(
              "Should change to " + Trackers.MIGRATION + " first when migrating from " +
                StoreFileTrackerFactory.getStoreFileTrackerName(oldTracker) + " for family " +
                newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          // here we do not check whether the table is disabled, as after changing to MIGRATION, we
          // still rely on the src SFT implementation to actually load the store files, so there
          // will be no data loss problem.
          Class<? extends StoreFileTracker> newSrcTracker =
            MigrationStoreFileTracker.getSrcTrackerClass(newConf);
          if (!oldTracker.equals(newSrcTracker)) {
            throw new DoNotRetryIOException("Should use src tracker " +
              StoreFileTrackerFactory.getStoreFileTrackerName(oldTracker) + " first but got " +
              StoreFileTrackerFactory.getStoreFileTrackerName(newSrcTracker) +
              " when migrating from " +
              StoreFileTrackerFactory.getStoreFileTrackerName(oldTracker) + " for family " +
              newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newDstTracker =
            MigrationStoreFileTracker.getDstTrackerClass(newConf);
          // the src and dst tracker should not be the same
          if (newSrcTracker.equals(newDstTracker)) {
            throw new DoNotRetryIOException("The src tracker and dst tracker are both " +
              StoreFileTrackerFactory.getStoreFileTrackerName(newSrcTracker) + " for family " +
              newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        }
      }
    }
  }

  /**
   * Makes sure restoring a snapshot does not break the current SFT setup follows
   * StoreUtils.createStoreConfiguration
   * @param currentTableDesc Existing Table's TableDescriptor
   * @param snapshotTableDesc Snapshot's TableDescriptor
   * @param baseConf Current global configuration
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
            "Restoring Snapshot is not possible because " + " the config for column family " +
              cfDesc.getNameAsString() + " has incompatible configuration. Current SFT: " +
              currentSFT + " SFT from snapshot: " + snapSFT);
        }
      }
    }
  }
}
