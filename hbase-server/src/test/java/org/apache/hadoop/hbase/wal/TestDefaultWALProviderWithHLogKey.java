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
package org.apache.hadoop.hbase.wal;


import java.util.NavigableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.regionserver.wal.HLogKey;

@Category({RegionServerTests.class, LargeTests.class})
public class TestDefaultWALProviderWithHLogKey extends TestFSHLogProvider {
  @Override
  WALKey getWalKey(final byte[] info, final TableName tableName, final long timestamp,
      final NavigableMap<byte[], Integer> scopes) {
    return new HLogKey(info, tableName, timestamp, mvcc, scopes);
  }
}
