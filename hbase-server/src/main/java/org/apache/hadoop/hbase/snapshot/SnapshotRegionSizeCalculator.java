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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.snapshot.SnapshotInfo.SnapshotStats;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * Utility class to calculate the size of each region in a snapshot.
 */
@InterfaceAudience.Private
public class SnapshotRegionSizeCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegionSizeCalculator.class);
  private final SnapshotManifest manifest;
  private final Configuration conf;
  private final Map<String, Long> regionSizes;

  private final FileSystem fs;

  public SnapshotRegionSizeCalculator(Configuration conf, SnapshotManifest manifest)
    throws IOException {
    this.conf = conf;
    this.manifest = manifest;
    this.regionSizes = calculateRegionSizes();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    fs = FileSystem.get(rootDir.toUri(), conf);
  }

  /**
   * Calculate the size of each region in the snapshot.
   * @return A map of region encoded names to their total size in bytes.
   */
  public Map<String, Long> calculateRegionSizes() throws IOException {
    SnapshotProtos.SnapshotDescription snapshot =
      SnapshotDescriptionUtils.readSnapshotInfo(fs, manifest.getSnapshotDir());
    SnapshotStats stats = SnapshotInfo.getSnapshotStats(conf, snapshot, null);
    return stats.getRegionSizeMap();
  }

  public long getRegionSize(String encodedRegionName) {
    Long size = regionSizes.get(encodedRegionName);
    if (size == null) {
      LOG.debug("Unknown region:" + encodedRegionName);
      return 0;
    } else {
      return size;
    }
  }

}
