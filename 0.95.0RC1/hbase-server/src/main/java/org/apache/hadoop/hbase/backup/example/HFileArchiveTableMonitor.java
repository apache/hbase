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
package org.apache.hadoop.hbase.backup.example;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Monitor the actual tables for which HFiles are archived for long-term retention (always kept
 * unless ZK state changes).
 * <p>
 * It is internally synchronized to ensure consistent view of the table state.
 */
public class HFileArchiveTableMonitor {
  private static final Log LOG = LogFactory.getLog(HFileArchiveTableMonitor.class);
  private final Set<String> archivedTables = new TreeSet<String>();

  /**
   * Set the tables to be archived. Internally adds each table and attempts to
   * register it.
   * <p>
   * <b>Note: All previous tables will be removed in favor of these tables.<b>
   * @param tables add each of the tables to be archived.
   */
  public synchronized void setArchiveTables(List<String> tables) {
    archivedTables.clear();
    archivedTables.addAll(tables);
  }

  /**
   * Add the named table to be those being archived. Attempts to register the
   * table
   * @param table name of the table to be registered
   */
  public synchronized void addTable(String table) {
    if (this.shouldArchiveTable(table)) {
      LOG.debug("Already archiving table: " + table + ", ignoring it");
      return;
    }
    archivedTables.add(table);
  }

  public synchronized void removeTable(String table) {
    archivedTables.remove(table);
  }

  public synchronized void clearArchive() {
    archivedTables.clear();
  }

  /**
   * Determine if the given table should or should not allow its hfiles to be deleted in the archive
   * @param tableName name of the table to check
   * @return <tt>true</tt> if its store files should be retained, <tt>false</tt> otherwise
   */
  public synchronized boolean shouldArchiveTable(String tableName) {
    return archivedTables.contains(tableName);
  }
}
