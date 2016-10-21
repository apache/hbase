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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.fs.legacy.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

/**
 * {@link BaseHFileCleanerDelegate} that only cleans HFiles that don't belong to a table that is
 * currently being archived.
 * <p>
 * This only works properly if the 
 * {@link org.apache.hadoop.hbase.fs.legacy.cleaner.TimeToLiveHFileCleaner}
 *  is also enabled (it always should be), since it may take a little time
 *  for the ZK notification to propagate, in which case we may accidentally
 *  delete some files.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class LongTermArchivingHFileCleaner extends BaseHFileCleanerDelegate {

  private static final Log LOG = LogFactory.getLog(LongTermArchivingHFileCleaner.class);

  TableHFileArchiveTracker archiveTracker;
  private FileSystem fs;

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    try {
      // if its a directory, then it can be deleted
      if (fStat.isDirectory()) return true;
      
      Path file = fStat.getPath();
      // check to see if
      FileStatus[] deleteStatus = FSUtils.listStatus(this.fs, file, null);
      // if the file doesn't exist, then it can be deleted (but should never
      // happen since deleted files shouldn't get passed in)
      if (deleteStatus == null) return true;

      // otherwise, we need to check the file's table and see its being archived
      Path family = file.getParent();
      Path region = family.getParent();
      Path table = region.getParent();

      String tableName = table.getName();
      boolean ret = !archiveTracker.keepHFiles(tableName);
      LOG.debug("Archiver says to [" + (ret ? "delete" : "keep") + "] files for table:" + tableName);
      return ret;
    } catch (IOException e) {
      LOG.error("Failed to lookup status of:" + fStat.getPath() + ", keeping it just incase.", e);
      return false;
    }
  }

  @Override
  public void setConf(Configuration config) {
    // setup our own zookeeper connection
    // Make my own Configuration. Then I'll have my own connection to zk that
    // I can close myself when comes time.
    Configuration conf = new Configuration(config);
    super.setConf(conf);
    try {
      this.fs = FileSystem.get(conf);
      this.archiveTracker = TableHFileArchiveTracker.create(conf);
      this.archiveTracker.start();
    } catch (KeeperException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @Override
  public void stop(String reason) {
    if (this.isStopped()) return;
    super.stop(reason);
    if (this.archiveTracker != null) {
      LOG.info("Stopping " + this.archiveTracker);
      this.archiveTracker.stop();
    }

  }

}
