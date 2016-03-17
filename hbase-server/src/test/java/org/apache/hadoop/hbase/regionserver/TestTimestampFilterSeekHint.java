/*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, LargeTests.class})
public class TestTimestampFilterSeekHint {

  private final static HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();
  private final static String RK = "myRK";
  private final static byte[] RK_BYTES = Bytes.toBytes(RK);

  private final static String FAMILY = "D";
  private final static byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);

  private final static String QUAL = "0";
  private final static byte[] QUAL_BYTES = Bytes.toBytes(QUAL);

  public static final int MAX_VERSIONS = 50000;
  private HRegion region;
  private int regionCount = 0;

  @Test
  public void testGetSeek() throws IOException {
    StoreFileScanner.instrument();
    prepareRegion();

    Get g = new Get(RK_BYTES);
    final TimestampsFilter timestampsFilter = new TimestampsFilter(ImmutableList.of(5L), true);
    g.setFilter(timestampsFilter);
    final long initialSeekCount = StoreFileScanner.getSeekCount();
    region.get(g);
    final long finalSeekCount = StoreFileScanner.getSeekCount();

    /*
      Make sure there's more than one.
      Aka one seek to get to the row, and one to get to the time.
    */
    assertTrue(finalSeekCount >= initialSeekCount + 3 );
  }

  @Test
  public void testGetDoesntSeekWithNoHint() throws IOException {
    StoreFileScanner.instrument();
    prepareRegion();

    Get g = new Get(RK_BYTES);
    g.setFilter(new TimestampsFilter(ImmutableList.of(5L)));
    final long initialSeekCount = StoreFileScanner.getSeekCount();
    region.get(g);
    final long finalSeekCount = StoreFileScanner.getSeekCount();

    assertTrue(finalSeekCount >= initialSeekCount );
    assertTrue(finalSeekCount < initialSeekCount + 3);
  }

  @Before
  public void prepareRegion() throws IOException {
    region =
        TEST_UTIL.createTestRegion("TestTimestampFilterSeekHint" + regionCount++,
            new HColumnDescriptor(FAMILY)
                .setBlocksize(1024)
                .setMaxVersions(MAX_VERSIONS)
        );

    for (long i = 0; i <MAX_VERSIONS - 2; i++) {
      Put p = new Put(RK_BYTES, i);
      p.add(FAMILY_BYTES, QUAL_BYTES, Bytes.toBytes(RandomStringUtils.randomAlphabetic(255)));
      region.put(p);
    }
    region.flushcache();
  }
}
