package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.commons.collections.keyvalue.AbstractMapEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;
import java.util.*;

public class MultiTableSnapshotInputFormatImpl extends TableSnapshotInputFormatImpl {

  public static final String KVP_DELIMITER = ":";

  public static final String RESTORE_DIRS_KEY = "hbase.MultiTableSnapshotInputFormat.restore.snapshotDirMapping";
  public static final String SNAPSHOT_TO_SCANS_KEY = "hbase.MultiTableSnapshotInputFormat.snapshotsToScans";


  public static List<InputSplit> getSplits(Configuration conf) throws IOException {
    List<InputSplit> rtn = Lists.newArrayList();

    Map<String, List<Scan>> snapshotsToScans = getSnapshotsToScans(conf);
    Map<String, Path> snapshotsToRestoreDirs = getSnapshotDirs(conf);
    for (Map.Entry<String, List<Scan>> entry : snapshotsToScans.entrySet()) {
      String snapshotName = entry.getKey();

      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);

      Path restoreDir = snapshotsToRestoreDirs.get(snapshotName);

      SnapshotManifest manifest = TableSnapshotInputFormatImpl.getSnapshotManifest(conf, snapshotName, rootDir, fs);
      List<HRegionInfo> regionInfos = TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest);

      int i = 0;
      for (Scan scan : entry.getValue()) {
        List<TableSnapshotInputFormatImpl.InputSplit> splits = TableSnapshotInputFormatImpl.getSplits(
            scan,
            manifest,
            regionInfos,
            restoreDir,
            conf);
        for (TableSnapshotInputFormatImpl.InputSplit split : splits) {
          rtn.add(split);
        }
        i++;
      }
    }
    return rtn;
  }

  public static Map<String, List<Scan>> getSnapshotsToScans(Configuration conf) throws IOException {

    Map<String, List<Scan>> rtn = Maps.newHashMap();

    for (Map.Entry<String, String> entry : getKeyValues(conf, SNAPSHOT_TO_SCANS_KEY)) {
      String snapshotName = entry.getKey();
      String scan = entry.getValue();

      List<Scan> snapshotScans = rtn.get(scan);
      if (snapshotScans == null) {
        snapshotScans = Lists.newArrayList();
        rtn.put(snapshotName, snapshotScans);
      }

      snapshotScans.add(TableMapReduceUtil.convertStringToScan(scan));
    }

    return rtn;
  }

  public static Map<String, Path> getSnapshotDirs(Configuration conf) throws IOException {
    List<Map.Entry<String, String>> kvps = getKeyValues(conf, RESTORE_DIRS_KEY);
    Map<String, Path> rtn = Maps.newHashMapWithExpectedSize(kvps.size());

    for (Map.Entry<String, String> kvp : kvps) {
      rtn.put(kvp.getKey(), new Path(kvp.getValue()));
    }

    return rtn;
  }

  /**
   * Configure conf to read from snapshotScans, with snapshots restored to a subdirectory of restoreDir.
   *
   * Sets: {@link #RESTORE_DIRS_KEY}, {@link #RESTORE_DIR_KEY}, {@link #SNAPSHOT_TO_SCANS_KEY}
   * @param conf
   * @param snapshotScans
   * @param restoreDir
   * @throws IOException
   */
  public static void setInput(Configuration conf, Map<String, Collection<Scan>> snapshotScans, Path restoreDir) throws IOException {

    conf.set(RESTORE_DIR_KEY, restoreDir.toString());

    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    Map<String, String> snapshotToRestoreDir = Maps.newHashMap();

    // flatten out snapshotScans for serialization to the job conf
    List<Map.Entry<String, String>> snapshotToSerializedScans = Lists.newArrayList();

    for (Map.Entry<String, Collection<Scan>> entry : snapshotScans.entrySet()) {
      String snapshotName = entry.getKey();
      Collection<Scan> scans = entry.getValue();

      // serialize all scans and map them to the appropriate snapshot
      for (Scan scan : scans) {
        snapshotToSerializedScans.add(new AbstractMap.SimpleImmutableEntry<>(
            snapshotName, TableMapReduceUtil.convertScanToString(scan)));
      }

      // handle snapshot dir + restoration
      Path restoreSnapshotDir = new Path(restoreDir, snapshotName + "__" + UUID.randomUUID().toString());
      snapshotToRestoreDir.put(snapshotName, restoreSnapshotDir.toString());

      // TODO: restore from record readers to parallelize.
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreSnapshotDir, snapshotName);
    }

    setKeyValues(conf, RESTORE_DIRS_KEY, snapshotToRestoreDir.entrySet());
    setKeyValues(conf, SNAPSHOT_TO_SCANS_KEY, snapshotToSerializedScans);
  }



  // TODO: these probably belong elsewhere/may already be implemented elsewhere.

  /**
   * Store a collection of Map.Entry's in conf, with each entry separated by ',' and key values delimited by ':'
   * @param conf
   * @param key
   * @param keyValues
   */
  private static void setKeyValues(Configuration conf, String key, Collection<Map.Entry<String, String>> keyValues) {
    List<String> serializedKvps = Lists.newArrayList();

    for (Map.Entry<String, String> kvp : keyValues) {
      serializedKvps.add(kvp.getKey() + KVP_DELIMITER + kvp.getValue());
    }

    conf.setStrings(key, serializedKvps.toArray(new String[serializedKvps.size()]));
  }

  private static List<Map.Entry<String, String>> getKeyValues(Configuration conf, String key) {
    String[] kvps = conf.getStrings(key);

    List<Map.Entry<String, String>> rtn = Lists.newArrayList();

    for (String kvp : kvps) {
      String[] split = kvp.split(KVP_DELIMITER);

      if (split.length != 2) {
        throw new IllegalArgumentException("Expected key value pair for configuration key '" + key + "'"
            + " to be of form '<key>:<value>; was " + kvp + " instead");
      }

      rtn.add(new AbstractMap.SimpleImmutableEntry<>(split[0], split[1]));
    }
    return rtn;
  }
}
