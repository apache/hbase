/*
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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: compType={0}")
public class TestMajorCompactionDataBlockEncoding extends MajorCompactionTestBase {

  public TestMajorCompactionDataBlockEncoding(String compType) {
    super(compType);
  }

  @TestTemplate
  public void testDataBlockEncodingInCacheOnly() throws Exception {
    majorCompactionWithDataBlockEncoding(true);
  }

  @TestTemplate
  public void testDataBlockEncodingEverywhere() throws Exception {
    majorCompactionWithDataBlockEncoding(false);
  }

  private void majorCompactionWithDataBlockEncoding(boolean inCacheOnly) throws Exception {
    Map<HStore, HFileDataBlockEncoder> replaceBlockCache = new HashMap<>();
    for (HStore store : r.getStores()) {
      HFileDataBlockEncoder blockEncoder = store.getDataBlockEncoder();
      replaceBlockCache.put(store, blockEncoder);
      final DataBlockEncoding inCache = DataBlockEncoding.PREFIX;
      final DataBlockEncoding onDisk = inCacheOnly ? DataBlockEncoding.NONE : inCache;
      ((HStore) store).setDataBlockEncoderInTest(new HFileDataBlockEncoderImpl(onDisk));
    }

    majorCompaction();

    // restore settings
    for (Entry<HStore, HFileDataBlockEncoder> entry : replaceBlockCache.entrySet()) {
      ((HStore) entry.getKey()).setDataBlockEncoderInTest(entry.getValue());
    }
  }
}
