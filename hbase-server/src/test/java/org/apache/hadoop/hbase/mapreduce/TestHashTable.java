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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Basic test for the HashTable M/R tool
 */
@Category(LargeTests.class)
public class TestHashTable {
  
  private static final Log LOG = LogFactory.getLog(TestHashTable.class);
  
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();  
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test
  public void testHashTable() throws Exception {
    final String tableName = "testHashTable";
    final byte[] family = Bytes.toBytes("family");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] column3 = Bytes.toBytes("c3");
    
    int numRows = 100;
    int numRegions = 10;
    int numHashFiles = 3;
    
    byte[][] splitRows = new byte[numRegions-1][];
    for (int i = 1; i < numRegions; i++) {
      splitRows[i-1] = Bytes.toBytes(numRows * i / numRegions);
    }
    
    long timestamp = 1430764183454L;
    // put rows into the first table
    HTable t1 = TEST_UTIL.createTable(TableName.valueOf(tableName), family, splitRows);
    for (int i = 0; i < numRows; i++) {
      Put p = new Put(Bytes.toBytes(i), timestamp);
      p.addColumn(family, column1, column1);
      p.addColumn(family, column2, column2);
      p.addColumn(family, column3, column3);
      t1.put(p);
    }
    t1.close();
    
    HashTable hashTable = new HashTable(TEST_UTIL.getConfiguration());
    
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS(tableName);
    
    long batchSize = 300;
    int code = hashTable.run(new String[] { 
        "--batchsize=" + batchSize,
        "--numhashfiles=" + numHashFiles,
        "--scanbatch=2",
        tableName,
        testDir.toString()});
    assertEquals("test job failed", 0, code);
    
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    
    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testDir);
    assertEquals(tableName, tableHash.tableName);
    assertEquals(batchSize, tableHash.batchSize);
    assertEquals(numHashFiles, tableHash.numHashFiles);
    assertEquals(numHashFiles - 1, tableHash.partitions.size());
    for (ImmutableBytesWritable bytes : tableHash.partitions) {
      LOG.debug("partition: " + Bytes.toInt(bytes.get()));
    }
    
    ImmutableMap<Integer, ImmutableBytesWritable> expectedHashes
      = ImmutableMap.<Integer, ImmutableBytesWritable>builder()
      .put(-1, new ImmutableBytesWritable(Bytes.fromHex("714cb10a9e3b5569852980edd8c6ca2f")))
      .put(5, new ImmutableBytesWritable(Bytes.fromHex("28d961d9252ce8f8d44a07b38d3e1d96")))
      .put(10, new ImmutableBytesWritable(Bytes.fromHex("f6bbc4a224d8fd929b783a92599eaffa")))
      .put(15, new ImmutableBytesWritable(Bytes.fromHex("522deb5d97f73a414ecc11457be46881")))
      .put(20, new ImmutableBytesWritable(Bytes.fromHex("b026f2611aaa46f7110116d807545352")))
      .put(25, new ImmutableBytesWritable(Bytes.fromHex("39ffc1a3094aa12a2e90ffd9cef2ce93")))
      .put(30, new ImmutableBytesWritable(Bytes.fromHex("f6b4d75727ce9a30ac29e4f08f601666")))
      .put(35, new ImmutableBytesWritable(Bytes.fromHex("422e2d2f1eb79a8f02171a705a42c090")))
      .put(40, new ImmutableBytesWritable(Bytes.fromHex("559ad61c900fffefea0a15abf8a97bc3")))
      .put(45, new ImmutableBytesWritable(Bytes.fromHex("23019084513eca41cee436b2a29611cb")))
      .put(50, new ImmutableBytesWritable(Bytes.fromHex("b40467d222ddb4949b142fe145ee9edc")))
      .put(55, new ImmutableBytesWritable(Bytes.fromHex("372bf89fcd8ca4b7ab3c1add9d07f7e4")))
      .put(60, new ImmutableBytesWritable(Bytes.fromHex("69ae0585e6255de27dce974e332b8f8b")))
      .put(65, new ImmutableBytesWritable(Bytes.fromHex("8029610044297aad0abdbecd485d8e59")))
      .put(70, new ImmutableBytesWritable(Bytes.fromHex("de5f784f7f78987b6e57ecfd81c8646f")))
      .put(75, new ImmutableBytesWritable(Bytes.fromHex("1cd757cc4e1715c8c3b1c24447a1ec56")))
      .put(80, new ImmutableBytesWritable(Bytes.fromHex("f9a53aacfeb6142b08066615e7038095")))
      .put(85, new ImmutableBytesWritable(Bytes.fromHex("89b872b7e639df32d3276b33928c0c91")))
      .put(90, new ImmutableBytesWritable(Bytes.fromHex("45eeac0646d46a474ea0484175faed38")))
      .put(95, new ImmutableBytesWritable(Bytes.fromHex("f57c447e32a08f4bf1abb2892839ac56")))
      .build();
  
    Map<Integer, ImmutableBytesWritable> actualHashes
      = new HashMap<Integer, ImmutableBytesWritable>();
    Path dataDir = new Path(testDir, HashTable.HASH_DATA_DIR);
    for (int i = 0; i < numHashFiles; i++) {
      Path hashPath = new Path(dataDir, HashTable.TableHash.getDataFileName(i));
      
      MapFile.Reader reader = new MapFile.Reader(hashPath, fs.getConf());
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      ImmutableBytesWritable hash = new ImmutableBytesWritable();
      while(reader.next(key, hash)) {
        String keyString = Bytes.toHex(key.get(), key.getOffset(), key.getLength());
        LOG.debug("Key: " + (keyString.isEmpty() ? "-1" : Integer.parseInt(keyString, 16))
            + " Hash: " + Bytes.toHex(hash.get(), hash.getOffset(), hash.getLength()));
        
        int intKey = -1;
        if (key.getLength() > 0) {
          intKey = Bytes.toInt(key.get(),  key.getOffset(), key.getLength());
        }
        if (actualHashes.containsKey(intKey)) {
          Assert.fail("duplicate key in data files: " + intKey);
        }
        actualHashes.put(intKey, new ImmutableBytesWritable(hash.copyBytes()));
      }
      reader.close();
    }
    
    FileStatus[] files = fs.listStatus(testDir);
    for (FileStatus file : files) {
      LOG.debug("Output file: " + file.getPath());
    }
    
    files = fs.listStatus(dataDir);
    for (FileStatus file : files) {
      LOG.debug("Data file: " + file.getPath());
    }
    
    if (!expectedHashes.equals(actualHashes)) {
      LOG.error("Diff: " + Maps.difference(expectedHashes, actualHashes));
    }
    Assert.assertEquals(expectedHashes, actualHashes);
    
    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.cleanupDataTestDirOnTestFS();
  }
  

}
