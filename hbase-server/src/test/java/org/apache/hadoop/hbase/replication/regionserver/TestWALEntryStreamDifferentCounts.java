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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream.HasNext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Try out different combinations of row count and KeyValue count
 */
public abstract class TestWALEntryStreamDifferentCounts extends WALEntryStreamTestBase {

  @Parameter(0)
  public int nbRows;

  @Parameter(1)
  public int walEditKVs;

  @Parameter(2)
  public boolean isCompressionEnabled;

  @Parameters(name = "{index}: nbRows={0}, walEditKVs={1}, isCompressionEnabled={2}")
  public static Iterable<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (int nbRows : new int[] { 1500, 60000 }) {
      for (int walEditKVs : new int[] { 1, 100 }) {
        for (boolean isCompressionEnabled : new boolean[] { false, true }) {
          params.add(new Object[] { nbRows, walEditKVs, isCompressionEnabled });
        }
      }
    }
    return params;
  }

  @Before
  public void setUp() throws IOException {
    CONF.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, isCompressionEnabled);
    initWAL();
  }

  @Test
  public void testDifferentCounts() throws Exception {
    mvcc.advanceTo(1);

    for (int i = 0; i < nbRows; i++) {
      appendToLogAndSync(walEditKVs);
    }

    log.rollWriter();

    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      int i = 0;
      while (entryStream.hasNext() == HasNext.YES) {
        assertNotNull(entryStream.next());
        i++;
      }
      assertEquals(nbRows, i);

      // should've read all entries, and since the last file is still opened for writing so we will
      // get a RETRY instead of NO here
      assertEquals(HasNext.RETRY, entryStream.hasNext());
    }
  }
}
