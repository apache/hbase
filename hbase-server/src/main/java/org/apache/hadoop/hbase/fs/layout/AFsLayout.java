/**
 *
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
package org.apache.hadoop.hbase.fs.layout;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystemFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;

public abstract class AFsLayout {
  protected final static Log LOG = LogFactory.getLog(AFsLayout.class);
  
  public abstract HRegionFileSystemFactory getHRegionFileSystemFactory();
  
  public abstract Path makeHFileLinkPath(SnapshotManifest snapshotManifest, HRegionInfo regionInfo, String familyName, String hfileName);

  public abstract Path getRegionArchiveDir(Path rootDir, TableName tableName, Path regiondir);

  public abstract Path getTableDirFromRegionDir(Path regionDir);
  
  public abstract List<Path> getRegionDirPaths(FileSystem fs, Path tableDir) throws IOException;

  public abstract List<FileStatus> getRegionDirFileStats(FileSystem fs, Path tableDir, FSUtils.RegionDirFilter filter) throws IOException;

  public abstract Path getRegionDir(Path tableDir, String name);

  protected AFsLayout() {
    super();
  }
}
