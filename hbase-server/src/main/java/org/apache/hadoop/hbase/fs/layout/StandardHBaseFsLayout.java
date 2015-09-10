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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystemFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.RegionDirFilter;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;


public class StandardHBaseFsLayout extends AFsLayout {
  private static final StandardHBaseFsLayout LAYOUT = new StandardHBaseFsLayout();
  
  public static StandardHBaseFsLayout get() { return LAYOUT; }
  
  private StandardHBaseFsLayout() { }
  
  @Override
  public HRegionFileSystemFactory getHRegionFileSystemFactory() {
    return new HRegionFileSystemFactory();
  }
  
  @Override
  public Path getRegionDir(Path tableDir, String name) {
    return new Path(tableDir, name);
  }

  @Override
  public List<FileStatus> getRegionDirFileStats(FileSystem fs, Path tableDir, RegionDirFilter filter)
          throws IOException {
    FileStatus[] rds = FSUtils.listStatus(fs, tableDir, filter);
    if (rds == null) {
      return null;
    }
    List<FileStatus> regionStatus = new ArrayList<FileStatus>(rds.length);
    for (FileStatus rdfs : rds) {
      regionStatus.add(rdfs);
    }
    return regionStatus;
  }

  /**
   * Given a particular table dir, return all the regiondirs inside it, excluding files such as
   * .tableinfo
   * @param fs A file system for the Path
   * @param tableDir Path to a specific table directory &lt;hbase.rootdir&gt;/&lt;tabledir&gt;
   * @return List of paths to valid region directories in table dir.
   * @throws IOException
   */
  @Override
  public List<Path> getRegionDirPaths(FileSystem fs, Path tableDir) throws IOException {
    // assumes we are in a table dir.
    FileStatus[] rds = fs.listStatus(tableDir, new FSUtils.RegionDirFilter(fs));
    List<Path> regionDirs = new ArrayList<Path>();
    for (FileStatus rdfs : rds) {
       regionDirs.add(rdfs.getPath());
    }
    return regionDirs;
  }
  
  @Override
  public Path getTableDirFromRegionDir(Path regionDir) {
    return regionDir.getParent();
  }

  /**
   * Get the archive directory for a given region under the specified table
   * @param tableName the table name. Cannot be null.
   * @param regiondir the path to the region directory. Cannot be null.
   * @return {@link Path} to the directory to archive the given region, or <tt>null</tt> if it
   *         should not be archived
   */
  @Override
  public Path getRegionArchiveDir(Path rootDir,
                                         TableName tableName,
                                         Path regiondir) {
    // get the archive directory for a table
    Path archiveDir = HFileArchiveUtil.getTableArchivePath(rootDir, tableName);
    // then add on the region path under the archive
    String encodedRegionName = regiondir.getName();
    return new Path(archiveDir, encodedRegionName);
  }

  @Override
  public Path makeHFileLinkPath(SnapshotManifest snapshotManifest, HRegionInfo regionInfo, String familyName, String hfileName) {
    return new Path(new Path(new Path(snapshotManifest.getSnapshotDir(),
      regionInfo.getEncodedName()), familyName), hfileName);
  }
}
