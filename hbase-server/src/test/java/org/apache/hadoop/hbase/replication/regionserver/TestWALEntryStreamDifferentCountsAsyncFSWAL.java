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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.Arguments;

@Tag(ReplicationTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(
    name = "{index}: nbRows={0}, walEditKVs={1}, isCompressionEnabled={2}")
public class TestWALEntryStreamDifferentCountsAsyncFSWAL extends TestWALEntryStreamDifferentCounts {

  public TestWALEntryStreamDifferentCountsAsyncFSWAL(int nbRows, int walEditKVs,
    boolean isCompressionEnabled) {
    super(nbRows, walEditKVs, isCompressionEnabled);
  }

  public static Stream<Arguments> parameters() {
    List<Arguments> params = new ArrayList<>();
    for (int nbRows : new int[] { 1500, 60000 }) {
      for (int walEditKVs : new int[] { 1, 100 }) {
        for (boolean isCompressionEnabled : new boolean[] { false, true }) {
          params.add(Arguments.of(nbRows, walEditKVs, isCompressionEnabled));
        }
      }
    }
    return params.stream();
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setClass(WALFactory.WAL_PROVIDER, AsyncFSWALProvider.class,
      AbstractFSWALProvider.class);
    startCluster();
  }
}
