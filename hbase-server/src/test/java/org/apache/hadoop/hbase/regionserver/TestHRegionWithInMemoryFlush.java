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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

/**
 * A test similar to TestHRegion, but with in-memory flush families.
 * Also checks wal truncation after in-memory compaction.
 */
@Category({VerySlowRegionServerTests.class, LargeTests.class})
public class TestHRegionWithInMemoryFlush extends TestHRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionWithInMemoryFlush.class);

  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  @Override
  public HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
      boolean isReadOnly, Durability durability, WAL wal, byte[]... families) throws IOException {
    boolean[] inMemory = new boolean[families.length];
    for(int i = 0; i < inMemory.length; i++) {
      inMemory[i] = true;
    }
    return TEST_UTIL.createLocalHRegionWithInMemoryFlags(tableName, startKey, stopKey,
        isReadOnly, durability, wal, inMemory, families);
  }
}

