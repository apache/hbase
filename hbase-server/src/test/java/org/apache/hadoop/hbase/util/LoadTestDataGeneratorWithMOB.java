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
package org.apache.hadoop.hbase.util;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.test.LoadTestKVGenerator;

/**
 * A load test data generator for MOB
 */
public class LoadTestDataGeneratorWithMOB
    extends MultiThreadedAction.DefaultDataGenerator {

  private byte[] mobColumnFamily;
  private LoadTestKVGenerator mobKvGenerator;

  public LoadTestDataGeneratorWithMOB(int minValueSize, int maxValueSize,
      int minColumnsPerKey, int maxColumnsPerKey, byte[]... columnFamilies) {
    super(minValueSize, maxValueSize, minColumnsPerKey, maxColumnsPerKey,
        columnFamilies);
  }

  public LoadTestDataGeneratorWithMOB(byte[]... columnFamilies) {
    super(columnFamilies);
  }

  @Override
  public void initialize(String[] args) {
    super.initialize(args);
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "LoadTestDataGeneratorWithMOB can have 3 arguments."
              + "1st argument is a column family, the 2nd argument "
              + "is the minimum mob data size and the 3rd argument "
              + "is the maximum mob data size.");
    }
    String mobColumnFamily = args[0];
    int minMobDataSize = Integer.parseInt(args[1]);
    int maxMobDataSize = Integer.parseInt(args[2]);
    configureMob(Bytes.toBytes(mobColumnFamily), minMobDataSize, maxMobDataSize);
  }

  private void configureMob(byte[] mobColumnFamily, int minMobDataSize,
      int maxMobDataSize) {
    this.mobColumnFamily = mobColumnFamily;
    mobKvGenerator = new LoadTestKVGenerator(minMobDataSize, maxMobDataSize);
  }

  @Override
  public byte[] generateValue(byte[] rowKey, byte[] cf,
      byte[] column) {
    if(Arrays.equals(cf, mobColumnFamily))
      return mobKvGenerator.generateRandomSizeValue(rowKey, cf, column);

    return super.generateValue(rowKey, cf, column);
  }
}
