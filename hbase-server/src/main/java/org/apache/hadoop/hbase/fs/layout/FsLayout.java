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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystemFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.RegionDirFilter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * This class exists mostly to allow us to access the layouts statically for convenience, as
 * though the layout classes are Util files. 
 */
public class FsLayout {
  private static final Log LOG = LogFactory.getLog(FsLayout.class);
  
  public static final String FS_LAYOUT_CHOICE = "hbase.fs.layout.choose";
  public static final String FS_LAYOUT_DETECT = "hbase.fs.layout.detect";
  public static final String FS_LAYOUT_DETECT_STRICT = "hbase.fs.layout.detect.strict";
  public static final String FS_LAYOUT_FILE_NAME = ".fslayout";
  
  // TODO: How can we avoid having a volatile variable (slightly slower reads)?
  // TODO: Move FsLayout class/contents into FSUtils?
  private static volatile AFsLayout fsLayout = null;
  
  public static final PathFilter FS_LAYOUT_PATHFILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return p.getName().equals(FS_LAYOUT_FILE_NAME);
    }
  };
  
  @VisibleForTesting
  static AFsLayout getRaw() {
    return fsLayout;
  }
  
  public static AFsLayout get() {
    AFsLayout curLayout = fsLayout;
    if (curLayout == null) {
      return initialize(null);
    } else {
      return curLayout;
    }
  }
  
  @VisibleForTesting
  public static void reset() {
    LOG.debug("Resetting FS layout to null");
    fsLayout = null;
  }
  
  @VisibleForTesting
  public static void setLayoutForTesting(@NonNull AFsLayout inputLayout) {
    LOG.debug("Setting FS layout to: " + inputLayout.getClass().getSimpleName());
    fsLayout = inputLayout; 
  }
  
  /*
   * TODO: Should this be required to be called manually?
   * Maybe call it manually in some processes (master/regionserver) and automatically everywhere else
   */
  @VisibleForTesting
  static synchronized AFsLayout initialize(Configuration conf) {
    try {
      if (fsLayout != null) {
        LOG.debug("Already initialized FS layout, not going to re-initialize");
        return fsLayout;
      }
      if (conf == null) {
        conf = HBaseConfiguration.create();
      }
      String choice = conf.get(FS_LAYOUT_CHOICE, null);
      boolean autodetect = conf.getBoolean(FS_LAYOUT_DETECT, false);
      if (choice != null && autodetect) {
        throw new IllegalStateException("Configuration both chooses a layout and "
            + "tries to automatically detect the layout");
      }
      if (choice != null) {
        Class<?> layoutClass = Class.forName(choice);
        Method getMethod = layoutClass.getMethod("get");
        return (AFsLayout) getMethod.invoke(null);
      }
      if (autodetect) {
        LOG.debug("Trying to detect hbase layout on filesystem");
        FileSystem fs = FSUtils.getCurrentFileSystem(conf);
        Path rootDir = FSUtils.getRootDir(conf);
        AFsLayout fsLayoutFromFile = readLayoutFile(fs, rootDir);
        if (fsLayoutFromFile == null) {
          if (conf.getBoolean(FS_LAYOUT_DETECT_STRICT, false)) {
            throw new IllegalStateException("Tried to detect fs layout, but there was no layout file at the root!");
          } else {
            LOG.debug("Didn't find a layout file, assuming classical hbase fs layout");
            fsLayout = StandardHBaseFsLayout.get();
          }
        } else {
          LOG.info("Detected hbase fs layout: " + fsLayoutFromFile.getClass().getSimpleName());
          fsLayout = fsLayoutFromFile;
        }
      } else {
        fsLayout = StandardHBaseFsLayout.get();
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return fsLayout;
  }
  
  public static AFsLayout readLayoutFile(FileSystem fs, Path rootDir) 
        throws FileNotFoundException, IOException, ClassNotFoundException, 
        NoSuchMethodException, SecurityException, IllegalAccessException, 
        IllegalArgumentException, InvocationTargetException {
    Path layoutFilePath = new Path(rootDir, FS_LAYOUT_FILE_NAME);
    FileStatus[] statuses = fs.listStatus(rootDir, FS_LAYOUT_PATHFILTER);
    if (statuses.length != 1) {
      return null;
    }
    FileStatus stat = statuses[0];
    int len = Ints.checkedCast(stat.getLen());
    byte[] inputStreamBytes = new byte[len];
    FSDataInputStream inputStream = fs.open(layoutFilePath);
    inputStream.readFully(inputStreamBytes);
    inputStream.close();
    String layoutClassName = Bytes.toString(inputStreamBytes);
    Class<?> layoutClass = Class.forName(layoutClassName);
    Method getMethod = layoutClass.getMethod("get");
    return (AFsLayout) getMethod.invoke(null);
  }
  
  public static void writeLayoutFile(FileSystem fs, Path rootDir, AFsLayout fsLayout, boolean overwrite) 
      throws IOException {
    Path layoutFilePath = new Path(rootDir, FS_LAYOUT_FILE_NAME);
    FSDataOutputStream outputStream = fs.create(layoutFilePath, overwrite);
    try {
      outputStream.write(Bytes.toBytes(fsLayout.getClass().getCanonicalName()));
    } finally {
      outputStream.close();
    }
  }
  
  public static boolean deleteLayoutFile(FileSystem fs, Path rootDir) throws IOException {
    Path layoutFilePath = new Path(rootDir, FS_LAYOUT_FILE_NAME);
    return fs.delete(layoutFilePath, false);
  }
  
  public static HRegionFileSystemFactory getHRegionFileSystemFactory() {
    return get().getHRegionFileSystemFactory();
  }
  
  public static Path getRegionDir(Path tableDir, HRegionInfo regionInfo) {
    return getRegionDir(tableDir, regionInfo.getEncodedName());
  }

  public static Path getRegionDir(Path tableDir, String name) {
    return get().getRegionDir(tableDir, name);
  }
  
  @Nullable
  public static List<FileStatus> getRegionDirFileStats(FileSystem fs, Path tableDir, RegionDirFilter filter) throws IOException {
    return get().getRegionDirFileStats(fs, tableDir, filter);
  }

  public static List<Path> getRegionDirPaths(FileSystem fs, Path tableDir) throws IOException {
    return get().getRegionDirPaths(fs, tableDir);
  }
  
  public static Path getTableDirFromRegionDir(Path regionDir) {
    return get().getTableDirFromRegionDir(regionDir);
  }

  public static Path getRegionArchiveDir(Path rootDir, TableName tableName, Path regiondir) {
    return get().getRegionArchiveDir(rootDir, tableName, regiondir);
  }
  
  public static Path makeHFileLinkPath(SnapshotManifest snapshotManifest, HRegionInfo regionInfo, String familyName, String hfileName) {
    return get().makeHFileLinkPath(snapshotManifest, regionInfo, familyName, hfileName);
  }
}
