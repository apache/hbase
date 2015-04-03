package org.apache.hadoop.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MultiTableSnapshotInputFormat extends TableSnapshotInputFormat implements InputFormat<ImmutableBytesWritable, Result> {

  private final MultiTableSnapshotInputFormatImpl delegate;

  public MultiTableSnapshotInputFormat() {
    this.delegate = new MultiTableSnapshotInputFormatImpl();
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits =
        delegate.getSplits(job);
    InputSplit[] results = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      results[i] = new TableSnapshotRegionSplit(splits.get(i));
    }
    return results;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new TableSnapshotRecordReader((TableSnapshotRegionSplit) split, job);
  }


  /**
   * Configure conf to read from snapshotScans, with snapshots restored to a subdirectory of restoreDir.
   *
   * Sets: {@link org.apache.hadoop.hbase.mapreduce.MultiTableSnapshotInputFormatImpl#RESTORE_DIRS_KEY},
   * {@link org.apache.hadoop.hbase.mapreduce.MultiTableSnapshotInputFormatImpl#SNAPSHOT_TO_SCANS_KEY}
   * @param conf
   * @param snapshotScans
   * @param restoreDir
   * @throws IOException
   */
  public static void setInput(Configuration conf, Map<String, Collection<Scan>> snapshotScans, Path restoreDir) throws IOException {
    new MultiTableSnapshotInputFormatImpl().setInput(conf, snapshotScans, restoreDir);
  }

}
