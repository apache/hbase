package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * MultiTableSnapshotInputFormat generalizes {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat}
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
 * {@link org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil#initMultiTableSnapshotMapperJob(java.util.Map, Class, Class, Class, org.apache.hadoop.mapreduce.Job, boolean, org.apache.hadoop.fs.Path)}
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
public class MultiTableSnapshotInputFormat extends TableSnapshotInputFormat {

  private final MultiTableSnapshotInputFormatImpl delegate;

  public MultiTableSnapshotInputFormat() {
    this.delegate = new MultiTableSnapshotInputFormatImpl();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits = delegate.getSplits(jobContext.getConfiguration());
    List<InputSplit> rtn = Lists.newArrayListWithCapacity(splits.size());

    for (TableSnapshotInputFormatImpl.InputSplit split : splits) {
      rtn.add(new TableSnapshotInputFormat.TableSnapshotRegionSplit(split));
    }

    return rtn;
  }


  public static void setInput(Configuration configuration, Map<String, Collection<Scan>> snapshotScans, Path tmpRestoreDir) throws IOException {
    new MultiTableSnapshotInputFormatImpl().setInput(configuration, snapshotScans, tmpRestoreDir);
  }
}
