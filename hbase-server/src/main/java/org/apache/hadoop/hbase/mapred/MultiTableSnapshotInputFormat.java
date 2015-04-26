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

/**
 * MultiTableSnapshotInputFormat generalizes {@link org.apache.hadoop.hbase.mapred.TableSnapshotInputFormat}
 * allowing a MapReduce job to run over one or more table snapshots, with one or more scans configured for each.
 * Internally, the input format delegates to {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat}
 * and thus has the same performance advantages; see {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat} for
 * more details.
 *

 * <p>
 * Usage is similar to TableSnapshotInputFormat, with the following exception: initMultiTableSnapshotMapperJob takes in a map
 * from snapshot name to a collection of scans. For each snapshot in the map, each corresponding scan will be applied;
 * the overall dataset for the job is defined by the concatenation of the regions and tables included in each snapshot/scan
 * pair.
 *
 * {@link org.apache.hadoop.hbase.mapred.TableMapReduceUtil#initMultiTableSnapshotMapperJob(Map, Class, Class, Class, JobConf, boolean, Path)}
 * can be used to configure the job.
 * <pre>{@code
 * Job job = new Job(conf);
 * Map<String, Collection<Scan>> snapshotScans = ImmutableMap.of(
 *    "snapshot1", ImmutableList.of(new Scan(Bytes.toBytes("a"), Bytes.toBytes("b"))),
 *    "snapshot2", ImmutableList.of(new Scan(Bytes.toBytes("1"), Bytes.toBytes("2")))
 * );
 * Path restoreDir = new Path("/tmp/snapshot_restore_dir")
 * TableMapReduceUtil.initTableSnapshotMapperJob(
 *     snapshotScans, MyTableMapper.class, MyMapKeyOutput.class,
 *      MyMapOutputValueWritable.class, job, true, restoreDir);
 * }
 * </pre>
 * <p>
 * Internally, this input format restores each snapshot into a subdirectory of the given tmp directory. Input splits and
 * record readers are created as described in {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat}
 * (one per region).
 * <p>
 *
 * See {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat} for more notes on permissioning; the
 * same caveats apply here.
 *
 * @see org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat
 * @see org.apache.hadoop.hbase.client.TableSnapshotScanner
 */
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
