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

package org.apache.hadoop.hbase.backup;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The administrative API for HBase Backup. Construct an instance and call {@link #close()}
 * afterwards.
 * <p>
 * BackupAdmin can be used to create backups, restore data from backups and for other
 * backup-related operations.
 * @since 2.0
 */
@InterfaceAudience.Private
public interface BackupAdmin extends Closeable {

  /**
   * Backup given list of tables fully. This is a synchronous operation. It returns backup id on
   * success or throw exception on failure.
   * @param userRequest BackupRequest instance
   * @return the backup Id
   */

  String backupTables(final BackupRequest userRequest) throws IOException;

  /**
   * Restore backup
   * @param request restore request
   * @throws IOException exception
   */
  void restore(RestoreRequest request) throws IOException;

  /**
   * Describe backup image command
   * @param backupId backup id
   * @return backup info
   * @throws IOException exception
   */
  BackupInfo getBackupInfo(String backupId) throws IOException;

  /**
   * Delete backup image command
   * @param backupIds array of backup ids
   * @return total number of deleted sessions
   * @throws IOException exception
   */
  int deleteBackups(String[] backupIds) throws IOException;

  /**
   * Merge backup images command
   * @param backupIds array of backup ids of images to be merged
   *        The resulting backup image will have the same backup id as the most
   *        recent image from a list of images to be merged
   * @throws IOException exception
   */
  void mergeBackups(String[] backupIds) throws IOException;

  /**
   * Show backup history command
   * @param n last n backup sessions
   * @return list of backup info objects
   * @throws IOException exception
   */
  List<BackupInfo> getHistory(int n) throws IOException;

  /**
   * Show backup history command with filters
   * @param n last n backup sessions
   * @param f list of filters
   * @return list of backup info objects
   * @throws IOException exception
   */
  List<BackupInfo> getHistory(int n, BackupInfo.Filter... f) throws IOException;

  /**
   * Backup sets list command - list all backup sets. Backup set is a named group of tables.
   * @return all registered backup sets
   * @throws IOException exception
   */
  List<BackupSet> listBackupSets() throws IOException;

  /**
   * Backup set describe command. Shows list of tables in this particular backup set.
   * @param name set name
   * @return backup set description or null
   * @throws IOException exception
   */
  BackupSet getBackupSet(String name) throws IOException;

  /**
   * Delete backup set command
   * @param name backup set name
   * @return true, if success, false - otherwise
   * @throws IOException exception
   */
  boolean deleteBackupSet(String name) throws IOException;

  /**
   * Add tables to backup set command
   * @param name name of backup set.
   * @param tables array of tables to be added to this set.
   * @throws IOException exception
   */
  void addToBackupSet(String name, TableName[] tables) throws IOException;

  /**
   * Remove tables from backup set
   * @param name name of backup set.
   * @param tables array of tables to be removed from this set.
   * @throws IOException exception
   */
  void removeFromBackupSet(String name, TableName[] tables) throws IOException;
}
