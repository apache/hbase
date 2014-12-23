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

package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;

/**
 * Similar to {@link RegionReplicaUtil} but for the server side
 */
public class ServerRegionReplicaUtil extends RegionReplicaUtil {

  /**
   * Returns the regionInfo object to use for interacting with the file system.
   * @return An HRegionInfo object to interact with the filesystem
   */
  public static HRegionInfo getRegionInfoForFs(HRegionInfo regionInfo) {
    if (regionInfo == null) {
      return null;
    }
    return RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
  }

  /**
   * Returns whether this region replica can accept writes.
   * @param region the HRegion object
   * @return whether the replica is read only
   */
  public static boolean isReadOnly(HRegion region) {
    return region.getTableDesc().isReadOnly()
      || !isDefaultReplica(region.getRegionInfo());
  }

  /**
   * Returns whether to replay the recovered edits to flush the results.
   * Currently secondary region replicas do not replay the edits, since it would
   * cause flushes which might affect the primary region. Primary regions even opened
   * in read only mode should replay the edits.
   * @param region the HRegion object
   * @return whether recovered edits should be replayed.
   */
  public static boolean shouldReplayRecoveredEdits(HRegion region) {
    return isDefaultReplica(region.getRegionInfo());
  }

  /**
   * Returns a StoreFileInfo from the given FileStatus. Secondary replicas refer to the
   * files of the primary region, so an HFileLink is used to construct the StoreFileInfo. This
   * way ensures that the secondary will be able to continue reading the store files even if
   * they are moved to archive after compaction
   * @throws IOException
   */
  public static StoreFileInfo getStoreFileInfo(Configuration conf, FileSystem fs,
      HRegionInfo regionInfo, HRegionInfo regionInfoForFs, String familyName, FileStatus status)
      throws IOException {

    // if this is a primary region, just return the StoreFileInfo constructed from path
    if (regionInfo.equals(regionInfoForFs)) {
      return new StoreFileInfo(conf, fs, status);
    }

    // else create a store file link. The link file does not exists on filesystem though.
    HFileLink link = HFileLink.build(conf, regionInfoForFs.getTable(),
            regionInfoForFs.getEncodedName(), familyName, status.getPath().getName());
    return new StoreFileInfo(conf, fs, status, link);
  }

}
