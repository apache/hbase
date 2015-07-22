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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The partitioner for the sweep job.
 * The key is a mob file name. We bucket by date.
 */
@InterfaceAudience.Private
public class MobFilePathHashPartitioner extends Partitioner<Text, KeyValue> {

  @Override
  public int getPartition(Text fileName, KeyValue kv, int numPartitions) {
    MobFileName mobFileName = MobFileName.create(fileName.toString());
    String date = mobFileName.getDate();
    int hash = date.hashCode();
    return (hash & Integer.MAX_VALUE) % numPartitions;
  }
}
