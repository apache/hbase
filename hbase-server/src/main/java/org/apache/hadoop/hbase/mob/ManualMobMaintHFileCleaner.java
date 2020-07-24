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
package org.apache.hadoop.hbase.mob;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BaseHFileCleanerDelegate} that prevents cleaning HFiles from a mob region
 *
 * keeps a map of table name strings to mob region name strings over the life of
 * a JVM instance. if there's churn of unique table names we'll eat memory until
 * Master restart.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ManualMobMaintHFileCleaner extends BaseHFileCleanerDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(ManualMobMaintHFileCleaner.class);

  // We need to avoid making HRegionInfo objects for every table we check.
  private static final ConcurrentMap<TableName, String> MOB_REGIONS = new ConcurrentHashMap<>();

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    try {
      // if its a directory, then it can be deleted
      if (fStat.isDirectory()) {
        return true;
      }

      Path file = fStat.getPath();

      // we need the table and region to determine if this is from a mob region
      // we don't need to worry about hfilelink back references, because the hfilelink cleaner will
      // retain them.
      Path family = file.getParent();
      Path region = family.getParent();
      Path table = region.getParent();

      TableName tableName = CommonFSUtils.getTableName(table);

      String mobRegion = MOB_REGIONS.get(tableName);
      if (mobRegion == null) {
        String tmp = MobUtils.getMobRegionInfo(tableName).getEncodedName();
        if (tmp == null) {
          LOG.error("couldn't determine mob region for table {} keeping files just in case.",
              tableName);
          return false;
        }
        mobRegion = MOB_REGIONS.putIfAbsent(tableName, tmp);
        // a return of null means that tmp is now in the map for future lookups.
        if (mobRegion == null) {
          mobRegion = tmp;
        }
        LOG.debug("Had to calculate name of mob region for table {} and it is {}", tableName,
            mobRegion);
      }

      boolean ret = !mobRegion.equals(region.getName());
      if (LOG.isDebugEnabled() && !ret) {
        LOG.debug("Keeping file '{}' because it is from mob dir", fStat.getPath());
      }
      return ret;
    } catch (RuntimeException e) {
      LOG.error("Failed to determine mob status of '{}', keeping it just in case.", fStat.getPath(),
          e);
      return false;
    }
  }

}
