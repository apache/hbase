package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MultiTableSnapshotInputFormatImpl extends TableSnapshotInputFormatImpl {

  public static List<TableSnapshotInputFormatImpl.InputSplit> getSplits(Configuration conf) throws IOException {
    List<TableSnapshotInputFormatImpl.InputSplit> rtn = Lists.newArrayList();

    Map<String, Collection<Scan>> snapshotsToScans = getSnapshotsToScans(conf);

    for (Map.Entry<String, Collection<Scan>> entry : snapshotsToScans.entrySet()) {
      String snapshotName = entry.getKey();

      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);

      SnapshotManifest manifest = TableSnapshotInputFormatImpl.getSnapshotManifest(conf, snapshotName, rootDir, fs);
      List<HRegionInfo> regionInfos = TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest);

      for (Scan scan : entry.getValue()) {
        List<TableSnapshotInputFormatImpl.InputSplit> splits = TableSnapshotInputFormatImpl.getSplits(
            scan,
            manifest,
            regionInfos,
            new Path(conf.get(RESTORE_DIR_KEY), snapshotName),
            conf);
        for (TableSnapshotInputFormatImpl.InputSplit split : splits) {
          rtn.add(split);
        }
      }
    }
    return rtn;
  }

  private static Map<String, Collection<Scan>> getSnapshotsToScans(Configuration conf) {
    return null;
  }

  public static void setInput(Configuration conf, Map<String, Collection<Scan>> snapshotScans, Path restoreDir) throws IOException {

    conf.set(RESTORE_DIR_KEY, restoreDir.toString());

    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);


    for (Map.Entry<String, Collection<Scan>> entry : snapshotScans.entrySet()) {
      String snapshotName = entry.getKey();
      Collection<Scan> scans = entry.getValue();


      restoreDir = new Path(restoreDir, snapshotName + "__" + UUID.randomUUID().toString());

      // TODO: restore from record readers to parallelize.
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
    }
  }
}
