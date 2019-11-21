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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Wrapper for the region FileSystem operations adding WAL specific operations
 */
@InterfaceAudience.Private
public class HRegionWALFileSystem extends HRegionFileSystem {

  HRegionWALFileSystem(Configuration conf, FileSystem fs, Path tableDir, RegionInfo regionInfo) {
    super(conf, fs, tableDir, regionInfo);
  }

  /**
   * Closes and archives the specified store files from the specified family.
   * @param familyName Family that contains the store filesMeta
   * @param storeFiles set of store files to remove
   * @throws IOException if the archiving fails
   */
  public void archiveRecoveredEdits(String familyName, Collection<HStoreFile> storeFiles)
    throws IOException {
    HFileArchiver.archiveRecoveredEdits(this.conf, this.fs, this.regionInfoForFs,
      Bytes.toBytes(familyName), storeFiles);
  }
}
