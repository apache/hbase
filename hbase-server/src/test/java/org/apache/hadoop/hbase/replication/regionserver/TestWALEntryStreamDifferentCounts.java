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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Try out different combinations of row count and KeyValue count
 */
public abstract class TestWALEntryStreamDifferentCounts extends WALEntryStreamTestBase {

  protected int nbRows;
  protected int walEditKVs;
  protected boolean isCompressionEnabled;

  protected TestWALEntryStreamDifferentCounts(int nbRows, int walEditKVs,
    boolean isCompressionEnabled) {
    this.nbRows = nbRows;
    this.walEditKVs = walEditKVs;
    this.isCompressionEnabled = isCompressionEnabled;
  }

  @BeforeEach
  public void setUp() throws IOException {
    CONF.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, isCompressionEnabled);
    initWAL();
  }

  @TestTemplate
  public void testDifferentCounts() throws Exception {
    mvcc.advanceTo(1);

    for (int i = 0; i < nbRows; i++) {
      appendToLogAndSync(walEditKVs);
    }

    log.rollWriter();

    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
      int i = 0;
      while (entryStream.hasNext()) {
        assertNotNull(entryStream.next());
        i++;
      }
      assertEquals(nbRows, i);

      // should've read all entries
      assertFalse(entryStream.hasNext());
    }
  }
}
