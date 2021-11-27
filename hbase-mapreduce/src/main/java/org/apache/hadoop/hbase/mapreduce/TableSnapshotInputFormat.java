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

package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * TableSnapshotInputFormat allows a MapReduce job to run over a table snapshot. The job
 * bypasses HBase servers, and directly accesses the underlying files (hfile, recovered edits,
 * wals, etc) directly to provide maximum performance. The snapshot is not required to be
 * restored to the live cluster or cloned. This also allows to run the mapreduce job from an
 * online or offline hbase cluster. The snapshot files can be exported by using the
 * {@link org.apache.hadoop.hbase.snapshot.ExportSnapshot} tool, to a pure-hdfs cluster,
 * and this InputFormat can be used to run the mapreduce job directly over the snapshot files.
 * The snapshot should not be deleted while there are jobs reading from snapshot files.
 * <p>
 * Usage is similar to TableInputFormat, and
 * {@link TableMapReduceUtil#initTableSnapshotMapperJob(String, Scan, Class, Class, Class, Job, boolean, Path)}
 * can be used to configure the job.
 * <pre>{@code
 * Job job = new Job(conf);
 * Scan scan = new Scan();
 * TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
 *      scan, MyTableMapper.class, MyMapKeyOutput.class,
 *      MyMapOutputValueWritable.class, job, true);
 * }
 * </pre>
 * <p>
 * Internally, this input format restores the snapshot into the given tmp directory. By default,
 * and similar to {@link TableInputFormat} an InputSplit is created per region, but optionally you
 * can run N mapper tasks per every region, in which case the region key range will be split to
 * N sub-ranges and an InputSplit will be created per sub-range. The region is opened for reading
 * from each RecordReader. An internal RegionScanner is used to execute the
 * {@link org.apache.hadoop.hbase.CellScanner} obtained from the user.
 * <p>
 * HBase owns all the data and snapshot files on the filesystem. Only the 'hbase' user can read from
 * snapshot files and data files.
 * To read from snapshot files directly from the file system, the user who is running the MR job
 * must have sufficient permissions to access snapshot and reference files.
 * This means that to run mapreduce over snapshot files, the MR job has to be run as the HBase
 * user or the user must have group or other privileges in the filesystem (See HBASE-8369).
 * Note that, given other users access to read from snapshot/data files will completely circumvent
 * the access control enforced by HBase.
 * @see org.apache.hadoop.hbase.client.TableSnapshotScanner
 */
@InterfaceAudience.Public
public class TableSnapshotInputFormat extends InputFormat<ImmutableBytesWritable, Result> {

  public static class TableSnapshotRegionSplit extends InputSplit implements Writable {
    private TableSnapshotInputFormatImpl.InputSplit delegate;

    // constructor for mapreduce framework / Writable
    public TableSnapshotRegionSplit() {
      this.delegate = new TableSnapshotInputFormatImpl.InputSplit();
    }

    public TableSnapshotRegionSplit(TableSnapshotInputFormatImpl.InputSplit delegate) {
      this.delegate = delegate;
    }

    public TableSnapshotRegionSplit(HTableDescriptor htd, HRegionInfo regionInfo,
        List<String> locations, Scan scan, Path restoreDir) {
      this.delegate =
          new TableSnapshotInputFormatImpl.InputSplit(htd, regionInfo, locations, scan, restoreDir);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return delegate.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return delegate.getLocations();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      delegate.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      delegate.readFields(in);
    }

    /**
     * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
     *             Use {@link #getRegion()}
     */
    @Deprecated
    public HRegionInfo getRegionInfo() {
      return delegate.getRegionInfo();
    }

    public RegionInfo getRegion() {
      return delegate.getRegionInfo();
    }

    TableSnapshotInputFormatImpl.InputSplit getDelegate() {
      return this.delegate;
    }
  }

  @InterfaceAudience.Private
  static class TableSnapshotRegionRecordReader extends
      RecordReader<ImmutableBytesWritable, Result> {
    private TableSnapshotInputFormatImpl.RecordReader delegate =
      new TableSnapshotInputFormatImpl.RecordReader();
    private TaskAttemptContext context;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.context = context;
      delegate.initialize(
        ((TableSnapshotRegionSplit) split).delegate,
        context.getConfiguration());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean result = delegate.nextKeyValue();
      if (result) {
        ScanMetrics scanMetrics = delegate.getScanner().getScanMetrics();
        if (scanMetrics != null && context != null) {
          TableRecordReaderImpl.updateCounters(scanMetrics, 0, context, 0);
        }
      }
      return result;
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
      return delegate.getCurrentKey();
    }

    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
      return delegate.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return delegate.getProgress();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new TableSnapshotRegionRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
    List<InputSplit> results = new ArrayList<>();
    for (TableSnapshotInputFormatImpl.InputSplit split :
        TableSnapshotInputFormatImpl.getSplits(job.getConfiguration())) {
      results.add(new TableSnapshotRegionSplit(split));
    }
    return results;
  }

  /**
   * Configures the job to use TableSnapshotInputFormat to read from a snapshot.
   * @param job the job to configure
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restoreDir can be deleted.
   * @throws IOException if an error occurs
   */
  public static void setInput(Job job, String snapshotName, Path restoreDir)
      throws IOException {
    TableSnapshotInputFormatImpl.setInput(job.getConfiguration(), snapshotName, restoreDir);
  }

  /**
   * Configures the job to use TableSnapshotInputFormat to read from a snapshot.
   * @param job the job to configure
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restoreDir can be deleted.
   * @param splitAlgo split algorithm to generate splits from region
   * @param numSplitsPerRegion how many input splits to generate per one region
   * @throws IOException if an error occurs
   */
   public static void setInput(Job job, String snapshotName, Path restoreDir,
                               RegionSplitter.SplitAlgorithm splitAlgo, int numSplitsPerRegion) throws IOException {
     TableSnapshotInputFormatImpl.setInput(job.getConfiguration(), snapshotName, restoreDir,
             splitAlgo, numSplitsPerRegion);
   }

  /**
   *  clean restore directory after snapshot scan job
   * @param job the snapshot scan job
   * @param snapshotName the name of the snapshot to read from
   * @throws IOException if an error occurs
   */
  public static void cleanRestoreDir(Job job, String snapshotName) throws IOException {
    TableSnapshotInputFormatImpl.cleanRestoreDir(job, snapshotName);
  }
}
