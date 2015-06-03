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

package org.apache.hadoop.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * MultiTableSnapshotInputFormat generalizes {@link org.apache.hadoop.hbase.mapred
 * .TableSnapshotInputFormat}
 * allowing a MapReduce job to run over one or more table snapshots, with one or more scans
 * configured for each.
 * Internally, the input format delegates to {@link org.apache.hadoop.hbase.mapreduce
 * .TableSnapshotInputFormat}
 * and thus has the same performance advantages; see {@link org.apache.hadoop.hbase.mapreduce
 * .TableSnapshotInputFormat} for
 * more details.
 * Usage is similar to TableSnapshotInputFormat, with the following exception:
 * initMultiTableSnapshotMapperJob takes in a map
 * from snapshot name to a collection of scans. For each snapshot in the map, each corresponding
 * scan will be applied;
 * the overall dataset for the job is defined by the concatenation of the regions and tables
 * included in each snapshot/scan
 * pair.
 * {@link org.apache.hadoop.hbase.mapred.TableMapReduceUtil#initMultiTableSnapshotMapperJob(Map,
 * Class, Class, Class, JobConf, boolean, Path)}
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
 * Internally, this input format restores each snapshot into a subdirectory of the given tmp
 * directory. Input splits and
 * record readers are created as described in {@link org.apache.hadoop.hbase.mapreduce
 * .TableSnapshotInputFormat}
 * (one per region).
 * See {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat} for more notes on
 * permissioning; the
 * same caveats apply here.
 *
 * @see org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat
 * @see org.apache.hadoop.hbase.client.TableSnapshotScanner
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MultiTableSnapshotInputFormat extends TableSnapshotInputFormat
    implements InputFormat<ImmutableBytesWritable, Result> {

  private final MultiTableSnapshotInputFormatImpl delegate;

  public MultiTableSnapshotInputFormat() {
    this.delegate = new MultiTableSnapshotInputFormatImpl();
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits = delegate.getSplits(job);
    InputSplit[] results = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      results[i] = new TableSnapshotRegionSplit(splits.get(i));
    }
    return results;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    return new TableSnapshotRecordReader((TableSnapshotRegionSplit) split, job);
  }

  /**
   * Configure conf to read from snapshotScans, with snapshots restored to a subdirectory of
   * restoreDir.
   * Sets: {@link org.apache.hadoop.hbase.mapreduce
   * .MultiTableSnapshotInputFormatImpl#RESTORE_DIRS_KEY},
   * {@link org.apache.hadoop.hbase.mapreduce
   * .MultiTableSnapshotInputFormatImpl#SNAPSHOT_TO_SCANS_KEY}
   *
   * @param conf
   * @param snapshotScans
   * @param restoreDir
   * @throws IOException
   */
  public static void setInput(Configuration conf, Map<String, Collection<Scan>> snapshotScans,
      Path restoreDir) throws IOException {
    new MultiTableSnapshotInputFormatImpl().setInput(conf, snapshotScans, restoreDir);
  }

}
