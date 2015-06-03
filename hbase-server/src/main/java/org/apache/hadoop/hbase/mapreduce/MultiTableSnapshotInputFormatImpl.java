/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.ConfigurationUtil;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared implementation of mapreduce code over multiple table snapshots.
 * Utilized by both mapreduce ({@link org.apache.hadoop.hbase.mapreduce
 * .MultiTableSnapshotInputFormat} and mapred
 * ({@link org.apache.hadoop.hbase.mapred.MultiTableSnapshotInputFormat} implementations.
 */
@InterfaceAudience.LimitedPrivate({ "HBase" })
@InterfaceStability.Evolving
public class MultiTableSnapshotInputFormatImpl {

  private static final Log LOG = LogFactory.getLog(MultiTableSnapshotInputFormatImpl.class);

  public static final String RESTORE_DIRS_KEY =
      "hbase.MultiTableSnapshotInputFormat.restore.snapshotDirMapping";
  public static final String SNAPSHOT_TO_SCANS_KEY =
      "hbase.MultiTableSnapshotInputFormat.snapshotsToScans";

  /**
   * Configure conf to read from snapshotScans, with snapshots restored to a subdirectory of
   * restoreDir.
   * Sets: {@link #RESTORE_DIRS_KEY}, {@link #SNAPSHOT_TO_SCANS_KEY}
   *
   * @param conf
   * @param snapshotScans
   * @param restoreDir
   * @throws IOException
   */
  public void setInput(Configuration conf, Map<String, Collection<Scan>> snapshotScans,
      Path restoreDir) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    setSnapshotToScans(conf, snapshotScans);
    Map<String, Path> restoreDirs =
        generateSnapshotToRestoreDirMapping(snapshotScans.keySet(), restoreDir);
    setSnapshotDirs(conf, restoreDirs);
    restoreSnapshots(conf, restoreDirs, fs);
  }

  /**
   * Return the list of splits extracted from the scans/snapshots pushed to conf by
   * {@link
   * #setInput(org.apache.hadoop.conf.Configuration, java.util.Map, org.apache.hadoop.fs.Path)}
   *
   * @param conf Configuration to determine splits from
   * @return Return the list of splits extracted from the scans/snapshots pushed to conf
   * @throws IOException
   */
  public List<TableSnapshotInputFormatImpl.InputSplit> getSplits(Configuration conf)
      throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    List<TableSnapshotInputFormatImpl.InputSplit> rtn = Lists.newArrayList();

    Map<String, Collection<Scan>> snapshotsToScans = getSnapshotsToScans(conf);
    Map<String, Path> snapshotsToRestoreDirs = getSnapshotDirs(conf);
    for (Map.Entry<String, Collection<Scan>> entry : snapshotsToScans.entrySet()) {
      String snapshotName = entry.getKey();

      Path restoreDir = snapshotsToRestoreDirs.get(snapshotName);

      SnapshotManifest manifest =
          TableSnapshotInputFormatImpl.getSnapshotManifest(conf, snapshotName, rootDir, fs);
      List<HRegionInfo> regionInfos =
          TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest);

      for (Scan scan : entry.getValue()) {
        List<TableSnapshotInputFormatImpl.InputSplit> splits =
            TableSnapshotInputFormatImpl.getSplits(scan, manifest, regionInfos, restoreDir, conf);
        rtn.addAll(splits);
      }
    }
    return rtn;
  }

  /**
   * Retrieve the snapshot name -> list<scan> mapping pushed to configuration by
   * {@link #setSnapshotToScans(org.apache.hadoop.conf.Configuration, java.util.Map)}
   *
   * @param conf Configuration to extract name -> list<scan> mappings from.
   * @return the snapshot name -> list<scan> mapping pushed to configuration
   * @throws IOException
   */
  public Map<String, Collection<Scan>> getSnapshotsToScans(Configuration conf) throws IOException {

    Map<String, Collection<Scan>> rtn = Maps.newHashMap();

    for (Map.Entry<String, String> entry : ConfigurationUtil
        .getKeyValues(conf, SNAPSHOT_TO_SCANS_KEY)) {
      String snapshotName = entry.getKey();
      String scan = entry.getValue();

      Collection<Scan> snapshotScans = rtn.get(snapshotName);
      if (snapshotScans == null) {
        snapshotScans = Lists.newArrayList();
        rtn.put(snapshotName, snapshotScans);
      }

      snapshotScans.add(TableMapReduceUtil.convertStringToScan(scan));
    }

    return rtn;
  }

  /**
   * Push snapshotScans to conf (under the key {@link #SNAPSHOT_TO_SCANS_KEY})
   *
   * @param conf
   * @param snapshotScans
   * @throws IOException
   */
  public void setSnapshotToScans(Configuration conf, Map<String, Collection<Scan>> snapshotScans)
      throws IOException {
    // flatten out snapshotScans for serialization to the job conf
    List<Map.Entry<String, String>> snapshotToSerializedScans = Lists.newArrayList();

    for (Map.Entry<String, Collection<Scan>> entry : snapshotScans.entrySet()) {
      String snapshotName = entry.getKey();
      Collection<Scan> scans = entry.getValue();

      // serialize all scans and map them to the appropriate snapshot
      for (Scan scan : scans) {
        snapshotToSerializedScans.add(new AbstractMap.SimpleImmutableEntry<>(snapshotName,
            TableMapReduceUtil.convertScanToString(scan)));
      }
    }

    ConfigurationUtil.setKeyValues(conf, SNAPSHOT_TO_SCANS_KEY, snapshotToSerializedScans);
  }

  /**
   * Retrieve the directories into which snapshots have been restored from
   * ({@link #RESTORE_DIRS_KEY})
   *
   * @param conf Configuration to extract restore directories from
   * @return the directories into which snapshots have been restored from
   * @throws IOException
   */
  public Map<String, Path> getSnapshotDirs(Configuration conf) throws IOException {
    List<Map.Entry<String, String>> kvps = ConfigurationUtil.getKeyValues(conf, RESTORE_DIRS_KEY);
    Map<String, Path> rtn = Maps.newHashMapWithExpectedSize(kvps.size());

    for (Map.Entry<String, String> kvp : kvps) {
      rtn.put(kvp.getKey(), new Path(kvp.getValue()));
    }

    return rtn;
  }

  public void setSnapshotDirs(Configuration conf, Map<String, Path> snapshotDirs) {
    Map<String, String> toSet = Maps.newHashMap();

    for (Map.Entry<String, Path> entry : snapshotDirs.entrySet()) {
      toSet.put(entry.getKey(), entry.getValue().toString());
    }

    ConfigurationUtil.setKeyValues(conf, RESTORE_DIRS_KEY, toSet.entrySet());
  }

  /**
   * Generate a random path underneath baseRestoreDir for each snapshot in snapshots and
   * return a map from the snapshot to the restore directory.
   *
   * @param snapshots      collection of snapshot names to restore
   * @param baseRestoreDir base directory under which all snapshots in snapshots will be restored
   * @return a mapping from snapshot name to the directory in which that snapshot has been restored
   */
  private Map<String, Path> generateSnapshotToRestoreDirMapping(Collection<String> snapshots,
      Path baseRestoreDir) {
    Map<String, Path> rtn = Maps.newHashMap();

    for (String snapshotName : snapshots) {
      Path restoreSnapshotDir =
          new Path(baseRestoreDir, snapshotName + "__" + UUID.randomUUID().toString());
      rtn.put(snapshotName, restoreSnapshotDir);
    }

    return rtn;
  }

  /**
   * Restore each (snapshot name, restore directory) pair in snapshotToDir
   *
   * @param conf          configuration to restore with
   * @param snapshotToDir mapping from snapshot names to restore directories
   * @param fs            filesystem to do snapshot restoration on
   * @throws IOException
   */
  public void restoreSnapshots(Configuration conf, Map<String, Path> snapshotToDir, FileSystem fs)
      throws IOException {
    // TODO: restore from record readers to parallelize.
    Path rootDir = FSUtils.getRootDir(conf);

    for (Map.Entry<String, Path> entry : snapshotToDir.entrySet()) {
      String snapshotName = entry.getKey();
      Path restoreDir = entry.getValue();
      LOG.info("Restoring snapshot " + snapshotName + " into " + restoreDir
          + " for MultiTableSnapshotInputFormat");
      restoreSnapshot(conf, snapshotName, rootDir, restoreDir, fs);
    }
  }

  void restoreSnapshot(Configuration conf, String snapshotName, Path rootDir, Path restoreDir,
      FileSystem fs) throws IOException {
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
  }

}
