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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Strategy for archiving files removed by {@link RestoreSnapshotHelper} during a restore/clone.
 * <p>
 * The shared restore/clone logic in {@link RestoreSnapshotHelper} only differs between the
 * server-side procedures and the MapReduce snapshot-scanning path in how removed files are
 * disposed of. Injecting the archiver lets both flows reuse the same core logic instead of forking
 * it: server-side callers use {@link #DEFAULT} (backed by {@link HFileArchiver}), while the
 * MapReduce module supplies its own implementation.
 */
@InterfaceAudience.Private
public interface RestoreSnapshotArchiver {

  /**
   * Archive the hfiles of an entire region that is being removed from the table directory.
   */
  void archiveRegion(Configuration conf, FileSystem fs, RegionInfo info, Path rootDir,
    Path tableDir) throws IOException;

  /**
   * Archive the store files of a single column family that is being removed from a region.
   */
  void archiveFamilyByFamilyDir(FileSystem fs, Configuration conf, RegionInfo parent,
    Path familyDir, byte[] family) throws IOException;

  /** Default archiver backed by {@link HFileArchiver}, used by server-side restore/clone. */
  RestoreSnapshotArchiver DEFAULT = new RestoreSnapshotArchiver() {
    @Override
    public void archiveRegion(Configuration conf, FileSystem fs, RegionInfo info, Path rootDir,
      Path tableDir) throws IOException {
      HFileArchiver.archiveRegion(conf, fs, info, rootDir, tableDir);
    }

    @Override
    public void archiveFamilyByFamilyDir(FileSystem fs, Configuration conf, RegionInfo parent,
      Path familyDir, byte[] family) throws IOException {
      HFileArchiver.archiveFamilyByFamilyDir(fs, conf, parent, familyDir, family);
    }
  };
}
