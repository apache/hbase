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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.annotations.VisibleForTesting;

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
 * {@link TableMapReduceUtil#initTableSnapshotMapperJob(String, Scan, Class, Class, Class, Job,
 *   boolean, Path)}
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
 * Internally, this input format restores the snapshot into the given tmp directory. Similar to
 * {@link TableInputFormat} an InputSplit is created per region. The region is opened for reading
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
@InterfaceStability.Evolving
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
        List<String> locations) {
      this.delegate = new TableSnapshotInputFormatImpl.InputSplit(htd, regionInfo, locations);
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
  }

  @VisibleForTesting
  static class TableSnapshotRegionRecordReader extends
      RecordReader<ImmutableBytesWritable, Result> {
    private TableSnapshotInputFormatImpl.RecordReader delegate =
      new TableSnapshotInputFormatImpl.RecordReader();
    private TaskAttemptContext context;
    private Method getCounter;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      this.context = context;
      getCounter = TableRecordReaderImpl.retrieveGetCounterWithStringsParams(context);
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
          TableRecordReaderImpl.updateCounters(scanMetrics, 0, getCounter, context, 0);
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
    List<InputSplit> results = new ArrayList<InputSplit>();
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
}
