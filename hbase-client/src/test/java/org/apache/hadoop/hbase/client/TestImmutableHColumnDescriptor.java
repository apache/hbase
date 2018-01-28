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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ClientTests.class, SmallTests.class})
public class TestImmutableHColumnDescriptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestImmutableHColumnDescriptor.class);

  @Rule
  public TestName name = new TestName();
  private static final List<Consumer<ImmutableHColumnDescriptor>> TEST_FUNCTION = Arrays.asList(
    hcd -> hcd.setValue("a", "a"),
    hcd -> hcd.setValue(Bytes.toBytes("a"), Bytes.toBytes("a")),
    hcd -> hcd.setConfiguration("aaa", "ccc"),
    hcd -> hcd.remove(Bytes.toBytes("aaa")),
    hcd -> hcd.removeConfiguration("xxx"),
    hcd -> hcd.setBlockCacheEnabled(false),
    hcd -> hcd.setBlocksize(10),
    hcd -> hcd.setBloomFilterType(BloomType.NONE),
    hcd -> hcd.setCacheBloomsOnWrite(false),
    hcd -> hcd.setCacheDataOnWrite(true),
    hcd -> hcd.setCacheIndexesOnWrite(true),
    hcd -> hcd.setCompactionCompressionType(Compression.Algorithm.LZO),
    hcd -> hcd.setCompressTags(true),
    hcd -> hcd.setCompressionType(Compression.Algorithm.LZO),
    hcd -> hcd.setDFSReplication((short) 10),
    hcd -> hcd.setDataBlockEncoding(DataBlockEncoding.NONE),
    hcd -> hcd.setEncryptionKey(Bytes.toBytes("xxx")),
    hcd -> hcd.setEncryptionType("xxx"),
    hcd -> hcd.setEvictBlocksOnClose(true),
    hcd -> hcd.setInMemory(true),
    hcd -> hcd.setInMemoryCompaction(MemoryCompactionPolicy.NONE),
    hcd -> hcd.setKeepDeletedCells(KeepDeletedCells.FALSE),
    hcd -> hcd.setMaxVersions(1000),
    hcd -> hcd.setMinVersions(10),
    hcd -> hcd.setMobCompactPartitionPolicy(MobCompactPartitionPolicy.DAILY),
    hcd -> hcd.setMobEnabled(true),
    hcd -> hcd.setMobThreshold(10),
    hcd -> hcd.setPrefetchBlocksOnOpen(true),
    hcd -> hcd.setScope(0),
    hcd -> hcd.setStoragePolicy("aaa"),
    hcd -> hcd.setTimeToLive(100),
    hcd -> hcd.setVersions(1, 10)
  );

  @Test
  public void testImmutable() {
    ImmutableHColumnDescriptor hcd = new ImmutableHColumnDescriptor(
      new HColumnDescriptor(Bytes.toBytes(name.getMethodName())));
    for (int i = 0; i != TEST_FUNCTION.size(); ++i) {
      try {
        TEST_FUNCTION.get(i).accept(hcd);
        fail("ImmutableHTableDescriptor can't be modified!!! The index of method is " + i);
      } catch (UnsupportedOperationException e) {
      }
    }
  }

  @Test
  public void testClassMethodsAreBuilderStyle() {
    BuilderStyleTest.assertClassesAreBuilderStyle(ImmutableHColumnDescriptor.class);
  }
}
