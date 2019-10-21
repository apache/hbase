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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test Index Chunk Pool's size configurable, See HBASE-23196
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestIndexChunkPoolSizeConfigurable {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIndexChunkPoolSizeConfigurable.class);

  @Test
  public void testIndexChunkPoolConfigurable() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreLAB.INDEX_CHUNK_PERCENTAGE_KEY, 0f);
    TEST_UTIL.startMiniCluster(1);
    assertFalse(ChunkCreator.getInstance().hasIndexChunkPool());
    assertTrue(ChunkCreator.getInstance().hasDataChunkPool());
    TEST_UTIL.shutdownMiniCluster();
  }
}
