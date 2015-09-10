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
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HierarchicalHRegionFileSystemFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.RegionDirFilter;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;


public class HierarchicalFsLayout extends AFsLayout {
  private static final String OLD_REGION_NAME_PADDING = "abcdef1234abcdef1234abcdef1234ab";
  private static final HierarchicalFsLayout LAYOUT = new HierarchicalFsLayout();
  
  static {
    assert OLD_REGION_NAME_PADDING.length() == 32;
  }
  
  public static HierarchicalFsLayout get() { return LAYOUT; }
  
  private HierarchicalFsLayout() { }
  
  @Override
  public HierarchicalHRegionFileSystemFactory getHRegionFileSystemFactory() {
    return new HierarchicalHRegionFileSystemFactory();
  }
  
  @Override
  public Path getRegionDir(Path tableDir, String name) {
    return getHumongousRegionDir(tableDir, name);
  }

  private Path getHumongousRegionDir(final Path tabledir, final String name) {
    if (name.length() != HRegionInfo.MD5_HEX_LENGTH) {
      String table = tabledir.getName();
      String namespace = tabledir.getParent().getName();
      
      // Meta and old root table use the old encoded name format still
      if (!namespace.equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
        throw new IllegalArgumentException("The region with encoded name " + name
          + " is not a humongous region, cannot get humongous region dir from it.");
      }
      if (!table.equals(TableName.META_TABLE_NAME.getQualifierAsString()) && 
          !table.equals(TableName.OLD_ROOT_TABLE_NAME.getQualifierAsString())) {
        throw new IllegalArgumentException("The region with encoded name " + name
          + " is not a humongous region, cannot get humongous region dir from it.");
      }
      
      // Add padding to guarantee we will have enough characters
      return new Path(new Path(tabledir, makeBucketName(name, OLD_REGION_NAME_PADDING)), name); 
    }
    return new Path(new Path(tabledir, makeBucketName(name, null)), name);
  }
  
  private String makeBucketName(String regionName, String padding) {
    if (padding != null) {
      regionName = regionName + padding;
    }
    return regionName.substring(HRegionInfo.MD5_HEX_LENGTH
      - HRegionFileSystem.HUMONGOUS_DIR_NAME_SIZE);
  }

  @Override
  public List<FileStatus> getRegionDirFileStats(FileSystem fs, Path tableDir, RegionDirFilter filter)
          throws IOException {
    FileStatus[] buckets = FSUtils.listStatus(fs, tableDir);
    if (buckets == null) {
      return null;
    }
    List<FileStatus> stats = new ArrayList<FileStatus>();
    for (FileStatus bucket : buckets) {
      FileStatus[] regionDirs = null;
      if (filter != null) {
        regionDirs = fs.listStatus(bucket.getPath(), filter);
      } else {
        regionDirs = fs.listStatus(bucket.getPath());
      }
      for (FileStatus regionDir : regionDirs) {
        stats.add(regionDir);
      }
    }
    if (stats.size() == 0) {
      return null;
    }
    return stats;
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
  public List<Path> getRegionDirPaths(final FileSystem fs, final Path tableDir) throws IOException {
    // assumes we are in a table dir.
    FileStatus[] rds = fs.listStatus(tableDir, new FSUtils.RegionDirFilter(fs));
    List<Path> regionDirs = new ArrayList<Path>();
    for (FileStatus rdfs : rds) {
      // get all region dirs from bucket dir
      FileStatus[] bucket_rds = fs.listStatus(rdfs.getPath(),
          new FSUtils.RegionDirFilter(fs));
      for (FileStatus bucket_rdfs : bucket_rds) {
        regionDirs.add(bucket_rdfs.getPath());
      }
    }
    return regionDirs;
  }
  
  @Override
  public Path getTableDirFromRegionDir(Path regionDir) {
    return regionDir.getParent().getParent();
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
    String parentName = regiondir.getParent().getName();
  
    return new Path(archiveDir, new Path(parentName, encodedRegionName));
  }

  @Override
  public Path makeHFileLinkPath(SnapshotManifest snapshotManifest, HRegionInfo regionInfo, String familyName, String hfileName) {
    return new Path(new Path(getHumongousRegionDir(snapshotManifest.getSnapshotDir(),
      regionInfo.getEncodedName()), familyName), hfileName);
  }
}
