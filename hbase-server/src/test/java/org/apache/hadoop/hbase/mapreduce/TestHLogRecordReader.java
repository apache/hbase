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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.hbase.mapreduce.WALInputFormat.WALRecordReader;
import org.apache.hadoop.hbase.mapreduce.HLogInputFormat.HLogKeyRecordReader;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.experimental.categories.Category;

/**
 * JUnit tests for the record reader in HLogInputFormat
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestHLogRecordReader extends TestWALRecordReader {

  @Override
  protected WALKey getWalKey(final long time) {
    return new HLogKey(info.getEncodedNameAsBytes(), tableName, time, mvcc);
  }

  @Override
  protected WALRecordReader getReader() {
    return new HLogKeyRecordReader();
  }
}
