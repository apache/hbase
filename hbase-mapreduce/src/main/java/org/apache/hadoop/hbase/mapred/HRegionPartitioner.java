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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * This is used to partition the output keys into groups of keys.
 * Keys are grouped according to the regions that currently exist
 * so that each reducer fills a single region so load is distributed.
 *
 * @param <K2>
 * @param <V2>
 */
@InterfaceAudience.Public
public class HRegionPartitioner<K2,V2>
implements Partitioner<ImmutableBytesWritable, V2> {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionPartitioner.class);
  // Connection and locator are not cleaned up; they just die when partitioner is done.
  private Connection connection;
  private RegionLocator locator;
  private byte[][] startKeys;

  @Override
  public void configure(JobConf job) {
    try {
      this.connection = ConnectionFactory.createConnection(HBaseConfiguration.create(job));
      TableName tableName = TableName.valueOf(job.get(TableOutputFormat.OUTPUT_TABLE));
      this.locator = this.connection.getRegionLocator(tableName);
    } catch (IOException e) {
      LOG.error(e.toString(), e);
    }

    try {
      this.startKeys = this.locator.getStartKeys();
    } catch (IOException e) {
      LOG.error(e.toString(), e);
    }
  }

  @Override
  public int getPartition(ImmutableBytesWritable key, V2 value, int numPartitions) {
    byte[] region = null;
    // Only one region return 0
    if (this.startKeys.length == 1){
      return 0;
    }
    try {
      // Not sure if this is cached after a split so we could have problems
      // here if a region splits while mapping
      region = locator.getRegionLocation(key.get()).getRegionInfo().getStartKey();
    } catch (IOException e) {
      LOG.error(e.toString(), e);
    }
    for (int i = 0; i < this.startKeys.length; i++){
      if (Bytes.compareTo(region, this.startKeys[i]) == 0 ){
        if (i >= numPartitions){
          // cover if we have less reduces then regions.
          return (Integer.toString(i).hashCode()
              & Integer.MAX_VALUE) % numPartitions;
        }
        return i;
      }
    }
    // if above fails to find start key that match we need to return something
    return 0;
  }
}
