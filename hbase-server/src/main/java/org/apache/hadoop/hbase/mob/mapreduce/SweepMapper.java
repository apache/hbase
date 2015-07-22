/**
 *
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
package org.apache.hadoop.hbase.mob.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.mapreduce.SweepJob.DummyMobAbortable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

/**
 * The mapper of a sweep job.
 * Takes the rows from the table and their results and map to <filename:Text, mobValue:KeyValue>
 * where mobValue is the actual cell in HBase.
 */
@InterfaceAudience.Private
public class SweepMapper extends TableMapper<Text, KeyValue> {

  private ZooKeeperWatcher zkw = null;

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    String id = context.getConfiguration().get(SweepJob.SWEEP_JOB_ID);
    String owner = context.getConfiguration().get(SweepJob.SWEEP_JOB_SERVERNAME);
    String sweeperNode = context.getConfiguration().get(SweepJob.SWEEP_JOB_TABLE_NODE);
    zkw = new ZooKeeperWatcher(context.getConfiguration(), id,
        new DummyMobAbortable());
    try {
      SweepJobNodeTracker tracker = new SweepJobNodeTracker(zkw, sweeperNode, owner);
      tracker.start();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    if (zkw != null) {
      zkw.close();
    }
  }

  @Override
  public void map(ImmutableBytesWritable r, Result columns, Context context) throws IOException,
      InterruptedException {
    if (columns == null) {
      return;
    }
    Cell[] cells = columns.rawCells();
    if (cells == null || cells.length == 0) {
      return;
    }
    for (Cell c : cells) {
      if (MobUtils.hasValidMobRefCellValue(c)) {
        String fileName = MobUtils.getMobFileName(c);
        context.write(new Text(fileName), KeyValueUtil.ensureKeyValue(c));
      }
    }
  }
}
