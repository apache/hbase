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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.layout.FsLayout;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;

/**
 * Test helper for testing archiving of HFiles
 */
public class HFileArchiveTestingUtil {

  private static final Log LOG = LogFactory.getLog(HFileArchiveTestingUtil.class);

  private HFileArchiveTestingUtil() {
    // NOOP private ctor since this is just a utility class
  }

  public static boolean compareArchiveToOriginal(FileStatus[] previous, FileStatus[] archived,
      FileSystem fs, boolean hasTimedBackup) {

    List<List<String>> lists = getFileLists(previous, archived);
    List<String> original = lists.get(0);
    Collections.sort(original);

    List<String> currentFiles = lists.get(1);
    Collections.sort(currentFiles);

    List<String> backedup = lists.get(2);
    Collections.sort(backedup);

    // check the backed up files versus the current (should match up, less the
    // backup time in the name)
    if (!hasTimedBackup == (backedup.size() > 0)) {
      LOG.debug("backedup files doesn't match expected.");
      return false;
    }
    String msg = null;
    if (hasTimedBackup) {
      msg = assertArchiveEquality(original, backedup);
      if (msg != null) {
        LOG.debug(msg);
        return false;
      }
    }
    msg = assertArchiveEquality(original, currentFiles);
    if (msg != null) {
      LOG.debug(msg);
      return false;
    }
    return true;
  }

  /**
   * Compare the archived files to the files in the original directory
   * @param expected original files that should have been archived
   * @param actual files that were archived
   * @param fs filessystem on which the archiving took place
   * @throws IOException
   */
  public static void assertArchiveEqualToOriginal(FileStatus[] expected, FileStatus[] actual,
      FileSystem fs) throws IOException {
    assertArchiveEqualToOriginal(expected, actual, fs, false);
  }

  /**
   * Compare the archived files to the files in the original directory
   * @param expected original files that should have been archived
   * @param actual files that were archived
   * @param fs {@link FileSystem} on which the archiving took place
   * @param hasTimedBackup <tt>true</tt> if we expect to find an archive backup directory with a
   *          copy of the files in the archive directory (and the original files).
   * @throws IOException
   */
  public static void assertArchiveEqualToOriginal(FileStatus[] expected, FileStatus[] actual,
      FileSystem fs, boolean hasTimedBackup) throws IOException {

    List<List<String>> lists = getFileLists(expected, actual);
    List<String> original = lists.get(0);
    Collections.sort(original);

    List<String> currentFiles = lists.get(1);
    Collections.sort(currentFiles);

    List<String> backedup = lists.get(2);
    Collections.sort(backedup);

    // check the backed up files versus the current (should match up, less the
    // backup time in the name)
    assertEquals("Didn't expect any backup files, but got: " + backedup, hasTimedBackup,
      backedup.size() > 0);
    String msg = null;
    if (hasTimedBackup) {
      assertArchiveEquality(original, backedup);
      assertNull(msg, msg);
    }

    // do the rest of the comparison
    msg = assertArchiveEquality(original, currentFiles);
    assertNull(msg, msg);
  }

  private static String assertArchiveEquality(List<String> expected, List<String> archived) {
    String compare = compareFileLists(expected, archived);
    if (!(expected.size() == archived.size())) return "Not the same number of current files\n"
        + compare;
    if (!expected.equals(archived)) return "Different backup files, but same amount\n" + compare;
    return null;
  }

  /**
   * @return <expected, gotten, backup>, where each is sorted
   */
  private static List<List<String>> getFileLists(FileStatus[] previous, FileStatus[] archived) {
    List<List<String>> files = new ArrayList<List<String>>();

    // copy over the original files
    List<String> originalFileNames = convertToString(previous);
    files.add(originalFileNames);

    List<String> currentFiles = new ArrayList<String>(previous.length);
    List<FileStatus> backedupFiles = new ArrayList<FileStatus>(previous.length);
    for (FileStatus f : archived) {
      String name = f.getPath().getName();
      // if the file has been backed up
      if (name.contains(".")) {
        Path parent = f.getPath().getParent();
        String shortName = name.split("[.]")[0];
        Path modPath = new Path(parent, shortName);
        FileStatus file = new FileStatus(f.getLen(), f.isDirectory(), f.getReplication(),
            f.getBlockSize(), f.getModificationTime(), modPath);
        backedupFiles.add(file);
      } else {
        // otherwise, add it to the list to compare to the original store files
        currentFiles.add(name);
      }
    }

    files.add(currentFiles);
    files.add(convertToString(backedupFiles));
    return files;
  }

  private static List<String> convertToString(FileStatus[] files) {
    return convertToString(Arrays.asList(files));
  }

  private static List<String> convertToString(List<FileStatus> files) {
    List<String> originalFileNames = new ArrayList<String>(files.size());
    for (FileStatus f : files) {
      originalFileNames.add(f.getPath().getName());
    }
    return originalFileNames;
  }

  /* Get a pretty representation of the differences */
  private static String compareFileLists(List<String> expected, List<String> gotten) {
    StringBuilder sb = new StringBuilder("Expected (" + expected.size() + "): \t\t Gotten ("
        + gotten.size() + "):\n");
    List<String> notFound = new ArrayList<String>();
    for (String s : expected) {
      if (gotten.contains(s)) sb.append(s + "\t\t" + s + "\n");
      else notFound.add(s);
    }
    sb.append("Not Found:\n");
    for (String s : notFound) {
      sb.append(s + "\n");
    }
    sb.append("\nExtra:\n");
    for (String s : gotten) {
      if (!expected.contains(s)) sb.append(s + "\n");
    }
    return sb.toString();
  }

  /**
   * Helper method to get the archive directory for the specified region
   * @param conf {@link Configuration} to check for the name of the archive directory
   * @param region region that is being archived
   * @return {@link Path} to the archive directory for the given region
   */
  public static Path getRegionArchiveDir(Configuration conf, HRegion region) throws IOException {
    return HFileArchiver.getRegionArchiveDir(FSUtils.getRootDir(conf),
        region.getTableDesc().getTableName(), region.getRegionFileSystem()
            .getRegionDir());
  }

  /**
   * Helper method to get the store archive directory for the specified region
   * @param conf {@link Configuration} to check for the name of the archive directory
   * @param region region that is being archived
   * @param store store that is archiving files
   * @return {@link Path} to the store archive directory for the given region
   */
  public static Path getStoreArchivePath(Configuration conf, HRegion region, Store store)
      throws IOException {
    return HFileArchiveUtil.getStoreArchivePath(conf, region.getRegionInfo(),
        region.getRegionFileSystem().getTableDir(), store.getFamily().getName());
  }

  public static Path getStoreArchivePath(HBaseTestingUtility util, String tableName,
      byte[] storeName) throws IOException {
    byte[] table = Bytes.toBytes(tableName);
    // get the RS and region serving our table
    List<HRegion> servingRegions = util.getHBaseCluster().getRegions(table);
    HRegion region = servingRegions.get(0);

    // check that we actually have some store files that were archived
    Store store = region.getStore(storeName);
    return HFileArchiveTestingUtil.getStoreArchivePath(util.getConfiguration(), region, store);
  }
}
