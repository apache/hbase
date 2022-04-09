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

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * MultiTableSnapshotInputFormat generalizes
 * {@link TableSnapshotInputFormat}
 * allowing a MapReduce job to run over one or more table snapshots, with one or more scans
 * configured for each.
 * Internally, the input format delegates to
 * {@link TableSnapshotInputFormat}
 * and thus has the same performance advantages;
 * see {@link TableSnapshotInputFormat} for
 * more details.
 * Usage is similar to TableSnapshotInputFormat, with the following exception:
 * initMultiTableSnapshotMapperJob takes in a map
 * from snapshot name to a collection of scans. For each snapshot in the map, each corresponding
 * scan will be applied;
 * the overall dataset for the job is defined by the concatenation of the regions and tables
 * included in each snapshot/scan
 * pair.
 * {@link TableMapReduceUtil#initMultiTableSnapshotMapperJob
 * (Map, Class, Class, Class, org.apache.hadoop.mapreduce.Job, boolean, Path)}
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
 * record readers are created as described in
 * {@link org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat}
 * (one per region).
 * See {@link TableSnapshotInputFormat} for more notes on
 * permissioning; the same caveats apply here.
 *
 * @see TableSnapshotInputFormat
 * @see org.apache.hadoop.hbase.client.TableSnapshotScanner
 */
@InterfaceAudience.Public
public class MultiTableSnapshotInputFormat extends TableSnapshotInputFormat {

  private final MultiTableSnapshotInputFormatImpl delegate;

  public MultiTableSnapshotInputFormat() {
    this.delegate = new MultiTableSnapshotInputFormatImpl();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    List<TableSnapshotInputFormatImpl.InputSplit> splits =
        delegate.getSplits(jobContext.getConfiguration());
    List<InputSplit> rtn = Lists.newArrayListWithCapacity(splits.size());

    for (TableSnapshotInputFormatImpl.InputSplit split : splits) {
      rtn.add(new TableSnapshotInputFormat.TableSnapshotRegionSplit(split));
    }

    return rtn;
  }

  public static void setInput(Configuration configuration,
      Map<String, Collection<Scan>> snapshotScans, Path tmpRestoreDir) throws IOException {
    new MultiTableSnapshotInputFormatImpl().setInput(configuration, snapshotScans, tmpRestoreDir);
  }
}
