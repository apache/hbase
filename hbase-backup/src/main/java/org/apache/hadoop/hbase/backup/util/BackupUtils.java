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

package org.apache.hadoop.hbase.backup.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection for methods used by multiple classes to backup HBase tables.
 */
@InterfaceAudience.Private
public final class BackupUtils {
  protected static final Logger LOG = LoggerFactory.getLogger(BackupUtils.class);
  public static final String LOGNAME_SEPARATOR = ".";
  public static final int MILLISEC_IN_HOUR = 3600000;

  private BackupUtils() {
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Loop through the RS log timestamp map for the tables, for each RS, find the min timestamp value
   * for the RS among the tables.
   * @param rsLogTimestampMap timestamp map
   * @return the min timestamp of each RS
   */
  public static HashMap<String, Long> getRSLogTimestampMins(
      HashMap<TableName, HashMap<String, Long>> rsLogTimestampMap) {
    if (rsLogTimestampMap == null || rsLogTimestampMap.isEmpty()) {
      return null;
    }

    HashMap<String, Long> rsLogTimestampMins = new HashMap<>();
    HashMap<String, HashMap<TableName, Long>> rsLogTimestampMapByRS = new HashMap<>();

    for (Entry<TableName, HashMap<String, Long>> tableEntry : rsLogTimestampMap.entrySet()) {
      TableName table = tableEntry.getKey();
      HashMap<String, Long> rsLogTimestamp = tableEntry.getValue();
      for (Entry<String, Long> rsEntry : rsLogTimestamp.entrySet()) {
        String rs = rsEntry.getKey();
        Long ts = rsEntry.getValue();
        if (!rsLogTimestampMapByRS.containsKey(rs)) {
          rsLogTimestampMapByRS.put(rs, new HashMap<>());
          rsLogTimestampMapByRS.get(rs).put(table, ts);
        } else {
          rsLogTimestampMapByRS.get(rs).put(table, ts);
        }
      }
    }

    for (Entry<String, HashMap<TableName, Long>> entry : rsLogTimestampMapByRS.entrySet()) {
      String rs = entry.getKey();
      rsLogTimestampMins.put(rs, BackupUtils.getMinValue(entry.getValue()));
    }

    return rsLogTimestampMins;
  }

  /**
   * copy out Table RegionInfo into incremental backup image need to consider move this logic into
   * HBackupFileSystem
   * @param conn connection
   * @param backupInfo backup info
   * @param conf configuration
   * @throws IOException exception
   */
  public static void copyTableRegionInfo(Connection conn, BackupInfo backupInfo, Configuration conf)
          throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    // for each table in the table set, copy out the table info and region
    // info files in the correct directory structure
    for (TableName table : backupInfo.getTables()) {
      if (!MetaTableAccessor.tableExists(conn, table)) {
        LOG.warn("Table " + table + " does not exists, skipping it.");
        continue;
      }
      TableDescriptor orig = FSTableDescriptors.getTableDescriptorFromFs(fs, rootDir, table);

      // write a copy of descriptor to the target directory
      Path target = new Path(backupInfo.getTableBackupDir(table));
      FileSystem targetFs = target.getFileSystem(conf);
      FSTableDescriptors descriptors =
          new FSTableDescriptors(conf, targetFs, FSUtils.getRootDir(conf));
      descriptors.createTableDescriptorForTableDirectory(target, orig, false);
      LOG.debug("Attempting to copy table info for:" + table + " target: " + target
          + " descriptor: " + orig);
      LOG.debug("Finished copying tableinfo.");
      List<RegionInfo> regions = MetaTableAccessor.getTableRegions(conn, table);
      // For each region, write the region info to disk
      LOG.debug("Starting to write region info for table " + table);
      for (RegionInfo regionInfo : regions) {
        Path regionDir =
            HRegion.getRegionDir(new Path(backupInfo.getTableBackupDir(table)), regionInfo);
        regionDir = new Path(backupInfo.getTableBackupDir(table), regionDir.getName());
        writeRegioninfoOnFilesystem(conf, targetFs, regionDir, regionInfo);
      }
      LOG.debug("Finished writing region info for table " + table);
    }
  }

  /**
   * Write the .regioninfo file on-disk.
   */
  public static void writeRegioninfoOnFilesystem(final Configuration conf, final FileSystem fs,
      final Path regionInfoDir, RegionInfo regionInfo) throws IOException {
    final byte[] content = RegionInfo.toDelimitedByteArray(regionInfo);
    Path regionInfoFile = new Path(regionInfoDir, "." + HConstants.REGIONINFO_QUALIFIER_STR);
    // First check to get the permissions
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // Write the RegionInfo file content
    FSDataOutputStream out = FSUtils.create(conf, fs, regionInfoFile, perms, null);
    try {
      out.write(content);
    } finally {
      out.close();
    }
  }

  /**
   * Parses hostname:port from WAL file path
   * @param p path to WAL file
   * @return hostname:port
   */
  public static String parseHostNameFromLogFile(Path p) {
    try {
      if (AbstractFSWALProvider.isArchivedLogFile(p)) {
        return BackupUtils.parseHostFromOldLog(p);
      } else {
        ServerName sname = AbstractFSWALProvider.getServerNameFromWALDirectoryName(p);
        if (sname != null) {
          return sname.getAddress().toString();
        } else {
          LOG.error("Skip log file (can't parse): " + p);
          return null;
        }
      }
    } catch (Exception e) {
      LOG.error("Skip log file (can't parse): " + p, e);
      return null;
    }
  }

  /**
   * Returns WAL file name
   * @param walFileName WAL file name
   * @return WAL file name
   */
  public static String getUniqueWALFileNamePart(String walFileName) {
    return getUniqueWALFileNamePart(new Path(walFileName));
  }

  /**
   * Returns WAL file name
   * @param p WAL file path
   * @return WAL file name
   */
  public static String getUniqueWALFileNamePart(Path p) {
    return p.getName();
  }

  /**
   * Get the total length of files under the given directory recursively.
   * @param fs The hadoop file system
   * @param dir The target directory
   * @return the total length of files
   * @throws IOException exception
   */
  public static long getFilesLength(FileSystem fs, Path dir) throws IOException {
    long totalLength = 0;
    FileStatus[] files = FSUtils.listStatus(fs, dir);
    if (files != null) {
      for (FileStatus fileStatus : files) {
        if (fileStatus.isDirectory()) {
          totalLength += getFilesLength(fs, fileStatus.getPath());
        } else {
          totalLength += fileStatus.getLen();
        }
      }
    }
    return totalLength;
  }

  /**
   * Get list of all old WAL files (WALs and archive)
   * @param c configuration
   * @param hostTimestampMap {host,timestamp} map
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getWALFilesOlderThan(final Configuration c,
      final HashMap<String, Long> hostTimestampMap) throws IOException {
    Path walRootDir = CommonFSUtils.getWALRootDir(c);
    Path logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<>();

    PathFilter filter = p -> {
      try {
        if (AbstractFSWALProvider.isMetaFile(p)) {
          return false;
        }
        String host = parseHostNameFromLogFile(p);
        if (host == null) {
          return false;
        }
        Long oldTimestamp = hostTimestampMap.get(host);
        Long currentLogTS = BackupUtils.getCreationTime(p);
        return currentLogTS <= oldTimestamp;
      } catch (Exception e) {
        LOG.warn("Can not parse" + p, e);
        return false;
      }
    };
    FileSystem walFs = CommonFSUtils.getWALFileSystem(c);
    logFiles = BackupUtils.getFiles(walFs, logDir, logFiles, filter);
    logFiles = BackupUtils.getFiles(walFs, oldLogDir, logFiles, filter);
    return logFiles;
  }

  public static TableName[] parseTableNames(String tables) {
    if (tables == null) {
      return null;
    }
    String[] tableArray = tables.split(BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);

    TableName[] ret = new TableName[tableArray.length];
    for (int i = 0; i < tableArray.length; i++) {
      ret[i] = TableName.valueOf(tableArray[i]);
    }
    return ret;
  }

  /**
   * Check whether the backup path exist
   * @param backupStr backup
   * @param conf configuration
   * @return Yes if path exists
   * @throws IOException exception
   */
  public static boolean checkPathExist(String backupStr, Configuration conf) throws IOException {
    boolean isExist = false;
    Path backupPath = new Path(backupStr);
    FileSystem fileSys = backupPath.getFileSystem(conf);
    String targetFsScheme = fileSys.getUri().getScheme();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Schema of given url: " + backupStr + " is: " + targetFsScheme);
    }
    if (fileSys.exists(backupPath)) {
      isExist = true;
    }
    return isExist;
  }

  /**
   * Check target path first, confirm it doesn't exist before backup
   * @param backupRootPath backup destination path
   * @param conf configuration
   * @throws IOException exception
   */
  public static void checkTargetDir(String backupRootPath, Configuration conf) throws IOException {
    boolean targetExists;
    try {
      targetExists = checkPathExist(backupRootPath, conf);
    } catch (IOException e) {
      String expMsg = e.getMessage();
      String newMsg = null;
      if (expMsg.contains("No FileSystem for scheme")) {
        newMsg =
            "Unsupported filesystem scheme found in the backup target url. Error Message: "
                + expMsg;
        LOG.error(newMsg);
        throw new IOException(newMsg);
      } else {
        throw e;
      }
    }

    if (targetExists) {
      LOG.info("Using existing backup root dir: " + backupRootPath);
    } else {
      LOG.info("Backup root dir " + backupRootPath + " does not exist. Will be created.");
    }
  }

  /**
   * Get the min value for all the Values a map.
   * @param map map
   * @return the min value
   */
  public static <T> Long getMinValue(HashMap<T, Long> map) {
    Long minTimestamp = null;
    if (map != null) {
      ArrayList<Long> timestampList = new ArrayList<>(map.values());
      Collections.sort(timestampList);
      // The min among all the RS log timestamps will be kept in backup system table table.
      minTimestamp = timestampList.get(0);
    }
    return minTimestamp;
  }

  /**
   * Parses host name:port from archived WAL path
   * @param p path
   * @return host name
   */
  public static String parseHostFromOldLog(Path p) {
    try {
      String n = p.getName();
      int idx = n.lastIndexOf(LOGNAME_SEPARATOR);
      String s = URLDecoder.decode(n.substring(0, idx), "UTF8");
      return ServerName.parseHostname(s) + ":" + ServerName.parsePort(s);
    } catch (Exception e) {
      LOG.warn("Skip log file (can't parse): " + p);
      return null;
    }
  }

  /**
   * Given the log file, parse the timestamp from the file name. The timestamp is the last number.
   * @param p a path to the log file
   * @return the timestamp
   * @throws IOException exception
   */
  public static Long getCreationTime(Path p) throws IOException {
    int idx = p.getName().lastIndexOf(LOGNAME_SEPARATOR);
    if (idx < 0) {
      throw new IOException("Cannot parse timestamp from path " + p);
    }
    String ts = p.getName().substring(idx + 1);
    return Long.parseLong(ts);
  }

  public static List<String> getFiles(FileSystem fs, Path rootDir, List<String> files,
      PathFilter filter) throws IOException {
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(rootDir, true);

    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (lfs.isDirectory()) {
        continue;
      }
      // apply filter
      if (filter.accept(lfs.getPath())) {
        files.add(lfs.getPath().toString());
      }
    }
    return files;
  }

  public static void cleanupBackupData(BackupInfo context, Configuration conf) throws IOException {
    cleanupHLogDir(context, conf);
    cleanupTargetDir(context, conf);
  }

  /**
   * Clean up directories which are generated when DistCp copying hlogs
   * @param backupInfo backup info
   * @param conf configuration
   * @throws IOException exception
   */
  private static void cleanupHLogDir(BackupInfo backupInfo, Configuration conf) throws IOException {
    String logDir = backupInfo.getHLogTargetDir();
    if (logDir == null) {
      LOG.warn("No log directory specified for " + backupInfo.getBackupId());
      return;
    }

    Path rootPath = new Path(logDir).getParent();
    FileSystem fs = FileSystem.get(rootPath.toUri(), conf);
    FileStatus[] files = listStatus(fs, rootPath, null);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      LOG.debug("Delete log files: " + file.getPath().getName());
      fs.delete(file.getPath(), true);
    }
  }

  private static void cleanupTargetDir(BackupInfo backupInfo, Configuration conf) {
    try {
      // clean up the data at target directory
      LOG.debug("Trying to cleanup up target dir : " + backupInfo.getBackupId());
      String targetDir = backupInfo.getBackupRootDir();
      if (targetDir == null) {
        LOG.warn("No target directory specified for " + backupInfo.getBackupId());
        return;
      }

      FileSystem outputFs = FileSystem.get(new Path(backupInfo.getBackupRootDir()).toUri(), conf);

      for (TableName table : backupInfo.getTables()) {
        Path targetDirPath =
            new Path(getTableBackupDir(backupInfo.getBackupRootDir(), backupInfo.getBackupId(),
              table));
        if (outputFs.delete(targetDirPath, true)) {
          LOG.info("Cleaning up backup data at " + targetDirPath.toString() + " done.");
        } else {
          LOG.info("No data has been found in " + targetDirPath.toString() + ".");
        }

        Path tableDir = targetDirPath.getParent();
        FileStatus[] backups = listStatus(outputFs, tableDir, null);
        if (backups == null || backups.length == 0) {
          outputFs.delete(tableDir, true);
          LOG.debug(tableDir.toString() + " is empty, remove it.");
        }
      }
      outputFs.delete(new Path(targetDir, backupInfo.getBackupId()), true);
    } catch (IOException e1) {
      LOG.error("Cleaning up backup data of " + backupInfo.getBackupId() + " at "
          + backupInfo.getBackupRootDir() + " failed due to " + e1.getMessage() + ".");
    }
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup1/backup_1396650096738/default/t1_dn/"
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @param tableName table name
   * @return backupPath String for the particular table
   */
  public static String getTableBackupDir(String backupRootDir, String backupId,
          TableName tableName) {
    return backupRootDir + Path.SEPARATOR + backupId + Path.SEPARATOR
        + tableName.getNamespaceAsString() + Path.SEPARATOR + tableName.getQualifierAsString()
        + Path.SEPARATOR;
  }

  /**
   * Sort history list by start time in descending order.
   * @param historyList history list
   * @return sorted list of BackupCompleteData
   */
  public static ArrayList<BackupInfo> sortHistoryListDesc(ArrayList<BackupInfo> historyList) {
    ArrayList<BackupInfo> list = new ArrayList<>();
    TreeMap<String, BackupInfo> map = new TreeMap<>();
    for (BackupInfo h : historyList) {
      map.put(Long.toString(h.getStartTs()), h);
    }
    Iterator<String> i = map.descendingKeySet().iterator();
    while (i.hasNext()) {
      list.add(map.get(i.next()));
    }
    return list;
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal This accommodates
   * differences between hadoop versions, where hadoop 1 does not throw a FileNotFoundException, and
   * return an empty FileStatus[] while Hadoop 2 will throw FileNotFoundException.
   * @param fs file system
   * @param dir directory
   * @param filter path filter
   * @return null if dir is empty or doesn't exist, otherwise FileStatus array
   */
  public static FileStatus[] listStatus(final FileSystem fs, final Path dir,
          final PathFilter filter) throws IOException {
    FileStatus[] status = null;
    try {
      status = filter == null ? fs.listStatus(dir) : fs.listStatus(dir, filter);
    } catch (FileNotFoundException fnfe) {
      // if directory doesn't exist, return null
      if (LOG.isTraceEnabled()) {
        LOG.trace(dir + " doesn't exist");
      }
    }

    if (status == null || status.length < 1) {
      return null;
    }

    return status;
  }

  /**
   * Return the 'path' component of a Path. In Hadoop, Path is an URI. This method returns the
   * 'path' component of a Path's URI: e.g. If a Path is
   * <code>hdfs://example.org:9000/hbase_trunk/TestTable/compaction.dir</code>, this method returns
   * <code>/hbase_trunk/TestTable/compaction.dir</code>. This method is useful if you want to print
   * out a Path without qualifying Filesystem instance.
   * @param p file system Path whose 'path' component we are to return.
   * @return Path portion of the Filesystem
   */
  public static String getPath(Path p) {
    return p.toUri().getPath();
  }

  /**
   * Given the backup root dir and the backup id, return the log file location for an incremental
   * backup.
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @return logBackupDir: ".../user/biadmin/backup1/WALs/backup_1396650096738"
   */
  public static String getLogBackupDir(String backupRootDir, String backupId) {
    return backupRootDir + Path.SEPARATOR + backupId + Path.SEPARATOR
        + HConstants.HREGION_LOGDIR_NAME;
  }

  private static List<BackupInfo> getHistory(Configuration conf, Path backupRootPath)
      throws IOException {
    // Get all (n) history from backup root destination

    FileSystem fs = FileSystem.get(backupRootPath.toUri(), conf);
    RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(backupRootPath);

    List<BackupInfo> infos = new ArrayList<>();
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();

      if (!lfs.isDirectory()) {
        continue;
      }

      String backupId = lfs.getPath().getName();
      try {
        BackupInfo info = loadBackupInfo(backupRootPath, backupId, fs);
        infos.add(info);
      } catch (IOException e) {
        LOG.error("Can not load backup info from: " + lfs.getPath(), e);
      }
    }
    // Sort
    Collections.sort(infos, new Comparator<BackupInfo>() {
      @Override
      public int compare(BackupInfo o1, BackupInfo o2) {
        long ts1 = getTimestamp(o1.getBackupId());
        long ts2 = getTimestamp(o2.getBackupId());

        if (ts1 == ts2) {
          return 0;
        }

        return ts1 < ts2 ? 1 : -1;
      }

      private long getTimestamp(String backupId) {
        String[] split = backupId.split("_");
        return Long.parseLong(split[1]);
      }
    });
    return infos;
  }

  public static List<BackupInfo> getHistory(Configuration conf, int n, Path backupRootPath,
      BackupInfo.Filter... filters) throws IOException {
    List<BackupInfo> infos = getHistory(conf, backupRootPath);
    List<BackupInfo> ret = new ArrayList<>();
    for (BackupInfo info : infos) {
      if (ret.size() == n) {
        break;
      }
      boolean passed = true;
      for (int i = 0; i < filters.length; i++) {
        if (!filters[i].apply(info)) {
          passed = false;
          break;
        }
      }
      if (passed) {
        ret.add(info);
      }
    }
    return ret;
  }

  public static BackupInfo loadBackupInfo(Path backupRootPath, String backupId, FileSystem fs)
      throws IOException {
    Path backupPath = new Path(backupRootPath, backupId);

    RemoteIterator<LocatedFileStatus> it = fs.listFiles(backupPath, true);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (lfs.getPath().getName().equals(BackupManifest.MANIFEST_FILE_NAME)) {
        // Load BackupManifest
        BackupManifest manifest = new BackupManifest(fs, lfs.getPath().getParent());
        BackupInfo info = manifest.toBackupInfo();
        return info;
      }
    }
    return null;
  }

  /**
   * Create restore request.
   * @param backupRootDir backup root dir
   * @param backupId backup id
   * @param check check only
   * @param fromTables table list from
   * @param toTables table list to
   * @param isOverwrite overwrite data
   * @return request obkect
   */
  public static RestoreRequest createRestoreRequest(String backupRootDir, String backupId,
      boolean check, TableName[] fromTables, TableName[] toTables, boolean isOverwrite) {
    RestoreRequest.Builder builder = new RestoreRequest.Builder();
    RestoreRequest request =
        builder.withBackupRootDir(backupRootDir).withBackupId(backupId).withCheck(check)
            .withFromTables(fromTables).withToTables(toTables).withOvewrite(isOverwrite).build();
    return request;
  }

  public static boolean validate(HashMap<TableName, BackupManifest> backupManifestMap,
      Configuration conf) throws IOException {
    boolean isValid = true;

    for (Entry<TableName, BackupManifest> manifestEntry : backupManifestMap.entrySet()) {
      TableName table = manifestEntry.getKey();
      TreeSet<BackupImage> imageSet = new TreeSet<>();

      ArrayList<BackupImage> depList = manifestEntry.getValue().getDependentListByTable(table);
      if (depList != null && !depList.isEmpty()) {
        imageSet.addAll(depList);
      }

      LOG.info("Dependent image(s) from old to new:");
      for (BackupImage image : imageSet) {
        String imageDir =
            HBackupFileSystem.getTableBackupDir(image.getRootDir(), image.getBackupId(), table);
        if (!BackupUtils.checkPathExist(imageDir, conf)) {
          LOG.error("ERROR: backup image does not exist: " + imageDir);
          isValid = false;
          break;
        }
        LOG.info("Backup image: " + image.getBackupId() + " for '" + table + "' is available");
      }
    }
    return isValid;
  }

  public static Path getBulkOutputDir(String tableName, Configuration conf, boolean deleteOnExit)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    String tmp = conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
            fs.getHomeDirectory() + "/hbase-staging");
    Path path =
        new Path(tmp + Path.SEPARATOR + "bulk_output-" + tableName + "-"
            + EnvironmentEdgeManager.currentTime());
    if (deleteOnExit) {
      fs.deleteOnExit(path);
    }
    return path;
  }

  public static Path getBulkOutputDir(String tableName, Configuration conf) throws IOException {
    return getBulkOutputDir(tableName, conf, true);
  }

  public static String getFileNameCompatibleString(TableName table) {
    return table.getNamespaceAsString() + "-" + table.getQualifierAsString();
  }

  public static boolean failed(int result) {
    return result != 0;
  }

  public static boolean succeeded(int result) {
    return result == 0;
  }

  public static BulkLoadHFiles createLoader(Configuration config) {
    // set configuration for restore:
    // LoadIncrementalHFile needs more time
    // <name>hbase.rpc.timeout</name> <value>600000</value>
    // calculates
    Configuration conf = new Configuration(config);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, MILLISEC_IN_HOUR);

    // By default, it is 32 and loader will fail if # of files in any region exceed this
    // limit. Bad for snapshot restore.
    conf.setInt(BulkLoadHFiles.MAX_FILES_PER_REGION_PER_FAMILY, Integer.MAX_VALUE);
    conf.set(BulkLoadHFiles.IGNORE_UNMATCHED_CF_CONF_KEY, "yes");
    return BulkLoadHFiles.create(conf);
  }

  public static String findMostRecentBackupId(String[] backupIds) {
    long recentTimestamp = Long.MIN_VALUE;
    for (String backupId : backupIds) {
      long ts = Long.parseLong(backupId.split("_")[1]);
      if (ts > recentTimestamp) {
        recentTimestamp = ts;
      }
    }
    return BackupRestoreConstants.BACKUPID_PREFIX + recentTimestamp;
  }

}
