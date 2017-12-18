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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for schema change procedures
 */
@InterfaceAudience.Private
public final class MasterDDLOperationHelper {
  private static final Logger LOG = LoggerFactory.getLogger(MasterDDLOperationHelper.class);

  private MasterDDLOperationHelper() {}

  /**
   * Remove the column family from the file system
   **/
  public static void deleteColumnFamilyFromFileSystem(
      final MasterProcedureEnv env,
      final TableName tableName,
      final List<RegionInfo> regionInfoList,
      final byte[] familyName,
      final boolean hasMob) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing family=" + Bytes.toString(familyName) + " from table=" + tableName);
    }
    for (RegionInfo hri : regionInfoList) {
      // Delete the family directory in FS for all the regions one by one
      mfs.deleteFamilyFromFS(hri, familyName);
    }
    if (hasMob) {
      // Delete the mob region
      Path mobRootDir = new Path(mfs.getRootDir(), MobConstants.MOB_DIR_NAME);
      RegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(tableName);
      mfs.deleteFamilyFromFS(mobRootDir, mobRegionInfo, familyName);
    }
  }
}
