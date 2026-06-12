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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.MapreduceHFileArchiver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce entry point for restoring a snapshot into a temporary directory for scanning.
 * <p>
 * This is a thin wrapper around {@link RestoreSnapshotHelper}: it adds a MapReduce-specific safety
 * guard that prevents the restore directory from pointing at (or under) the live HBase root
 * directory, then delegates the actual restore/clone to {@link RestoreSnapshotHelper}, injecting the
 * MapReduce-local {@link MapreduceHFileArchiver} so that no core restore logic is duplicated here.
 * <p>
 * The extra guard matters because a misconfigured {@code restoreDir} under {@code hbase.rootdir/data}
 * would let the MapReduce job archive (and ultimately delete) production HFiles. The server-side
 * restore/clone procedures legitimately operate against the production root directory, so this
 * stricter validation is layered only on the MapReduce path rather than in
 * {@link RestoreSnapshotHelper} itself. See HBASE-29435.
 */
@InterfaceAudience.Private
public final class MapreduceRestoreSnapshotHelper {

  private static final Logger LOG =
    LoggerFactory.getLogger(MapreduceRestoreSnapshotHelper.class);

  private MapreduceRestoreSnapshotHelper() {
  }

  /**
   * Copy the snapshot files for a snapshot scanner, discards meta changes.
   * <p>
   * Rejects a {@code restoreDir} that equals or is nested under {@code rootDir} before delegating to
   * {@link RestoreSnapshotHelper#copySnapshotForScanner}, which performs the restore using the
   * MapReduce-local archiver.
   */
  public static RestoreSnapshotHelper.RestoreMetaChanges copySnapshotForScanner(Configuration conf,
    FileSystem fs, Path rootDir, Path restoreDir, String snapshotName) throws IOException {
    // Guard (HBASE-29435): the restore directory must not be the HBase root directory or a
    // sub directory of it, otherwise the MR job could archive and permanently delete production
    // HFiles once the HFileCleaner TTL elapses.
    String rootPath = rootDir.toUri().getPath();
    String restorePath = restoreDir.toUri().getPath();
    if (restorePath.equals(rootPath) || restorePath.startsWith(rootPath + "/")) {
      String message = "BLOCKED: MapReduce restore directory cannot be the HBase root directory "
        + "or a sub directory of it. This could lead to accidental archival and permanent "
        + "data loss if the path falls under " + rootDir + "/data/. Use a temporary directory "
        + "outside of hbase.rootdir for MR snapshot scanning. RootDir: " + rootDir
        + ", restoreDir: " + restoreDir;
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }

    return RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName,
      new MapreduceHFileArchiver());
  }
}
