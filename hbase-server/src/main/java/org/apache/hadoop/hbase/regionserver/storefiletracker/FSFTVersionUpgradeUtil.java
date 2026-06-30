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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
final class FSFTVersionUpgradeUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FSFTVersionUpgradeUtil.class);

  private FSFTVersionUpgradeUtil() {
  }

  static boolean familyDirNeedsUpgrade(FileSystem fs, Path familyDir, long targetVersion)
    throws IOException {
    Path trackDir = new Path(familyDir, StoreFileListFile.TRACK_FILE_DIR);
    StoreFileList storeFileList = getLatestTrackFile(fs, trackDir);
    if (storeFileList == null || !storeFileList.hasVersion()) {
      return false;
    }
    return storeFileList.getVersion() < targetVersion;
  }

  
	private static StoreFileList getLatestTrackFile(FileSystem fs, Path trackerFileDir) throws IOException {
		StoreFileList[] lists = new StoreFileList[2];
		NavigableMap<Long, List<Path>> seqId2TrackFiles = StoreFileListFile.listFiles(fs, trackerFileDir);
		for (Map.Entry<Long, List<Path>> entry : seqId2TrackFiles.entrySet()) {
			List<Path> files = entry.getValue();
			// should not have more than 2 files, if not, it means that the track files are
			// broken, just
			// throw exception out and fail the region open.
			if (files.size() > 2) {
				throw new DoNotRetryIOException("Should only have at most 2 track files for sequence id "
						+ entry.getKey() + ", but got " + files.size() + " files: " + files);
			}
			for (int i = 0; i < files.size(); i++) {
				try {
					lists[i] = StoreFileListFile.load(fs, files.get(i));
				} catch (EOFException e) {
					// this is normal case, so just log at debug
					LOG.debug("EOF loading track file ignoring the exception", e);
				}
			}
		}
		return lists[StoreFileListFile.select(lists)];
	}
  

}

