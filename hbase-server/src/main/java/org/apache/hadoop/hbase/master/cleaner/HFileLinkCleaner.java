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
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * HFileLink cleaner that determines if a hfile should be deleted.
 * HFiles can be deleted only if there're no links to them.
 *
 * When a HFileLink is created a back reference file is created in:
 *      /hbase/archive/table/region/cf/.links-hfile/ref-region.ref-table
 * To check if the hfile can be deleted the back references folder must be empty.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class HFileLinkCleaner extends BaseHFileCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(HFileLinkCleaner.class);

  private FileSystem fs = null;

  @Override
  public synchronized boolean isFileDeletable(FileStatus fStat) {
    if (this.fs == null) return false;
    Path filePath = fStat.getPath();
    // HFile Link is always deletable
    if (HFileLink.isHFileLink(filePath)) return true;

    // If the file is inside a link references directory, means that it is a back ref link.
    // The back ref can be deleted only if the referenced file doesn't exists.
    Path parentDir = filePath.getParent();
    if (HFileLink.isBackReferencesDir(parentDir)) {
      Path hfilePath = null;
      try {
        // Also check if the HFile is in the HBASE_TEMP_DIRECTORY; this is where the referenced
        // file gets created when cloning a snapshot.
        hfilePath = HFileLink.getHFileFromBackReference(
            new Path(FSUtils.getRootDir(getConf()), HConstants.HBASE_TEMP_DIRECTORY), filePath);
        if (fs.exists(hfilePath)) {
          return false;
        }
        // check whether the HFileLink still exists in mob dir.
        hfilePath = HFileLink.getHFileFromBackReference(MobUtils.getMobHome(getConf()), filePath);
        if (fs.exists(hfilePath)) {
          return false;
        }
        hfilePath = HFileLink.getHFileFromBackReference(FSUtils.getRootDir(getConf()), filePath);
        return !fs.exists(hfilePath);
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Couldn't verify if the referenced file still exists, keep it just in case: "
              + hfilePath);
        }
        return false;
      }
    }

    // HFile is deletable only if has no links
    Path backRefDir = null;
    try {
      backRefDir = HFileLink.getBackReferencesDir(parentDir, filePath.getName());
      return FSUtils.listStatus(fs, backRefDir) == null;
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't get the references, not deleting file, just in case. filePath="
            + filePath + ", backRefDir=" + backRefDir);
      }
      return false;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    // setup filesystem
    try {
      this.fs = FileSystem.get(this.getConf());
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't instantiate the file system, not deleting file, just in case. "
            + FileSystem.FS_DEFAULT_NAME_KEY + "="
            + getConf().get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS));
      }
    }
  }
}
