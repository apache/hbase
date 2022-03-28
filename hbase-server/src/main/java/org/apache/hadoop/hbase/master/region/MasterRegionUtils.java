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
package org.apache.hadoop.hbase.master.region;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
final class MasterRegionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegionUtils.class);

  private MasterRegionUtils() {
  }

  static void moveFilesUnderDir(FileSystem fs, Path src, Path dst, String suffix)
    throws IOException {
    if (!fs.exists(dst) && !fs.mkdirs(dst)) {
      LOG.warn("Failed to create dir {}", dst);
      return;
    }
    FileStatus[] archivedWALFiles = fs.listStatus(src);
    if (archivedWALFiles == null) {
      return;
    }
    for (FileStatus status : archivedWALFiles) {
      Path file = status.getPath();
      Path newFile = new Path(dst, file.getName() + suffix);
      if (fs.rename(file, newFile)) {
        LOG.info("Moved {} to {}", file, newFile);
      } else {
        LOG.warn("Failed to move from {} to {}", file, newFile);
      }
    }
  }
}
