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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helper class for schema change procedures
 */
@InterfaceAudience.Private
public final class MasterDDLOperationHelper {
  private static final Log LOG = LogFactory.getLog(MasterDDLOperationHelper.class);

  private MasterDDLOperationHelper() {}

  /**
   * Remove the column family from the file system
   **/
  public static void deleteColumnFamilyFromFileSystem(
      final MasterProcedureEnv env,
      final TableName tableName,
      final List<HRegionInfo> regionInfoList,
      final byte[] familyName,
      final boolean hasMob) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing family=" + Bytes.toString(familyName) + " from table=" + tableName);
    }
    for (HRegionInfo hri : regionInfoList) {
      // Delete the family directory in FS for all the regions one by one
      mfs.deleteFamilyFromFS(hri, familyName);
    }
    if (hasMob) {
      // Delete the mob region
      Path mobRootDir = new Path(mfs.getRootDir(), MobConstants.MOB_DIR_NAME);
      HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(tableName);
      mfs.deleteFamilyFromFS(mobRootDir, mobRegionInfo, familyName);
    }
  }
}
