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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * Basic test for the HashTable M/R tool
 */
@Tag(LargeTests.TAG)
public class TestHashTable {

  private static final Logger LOG = LoggerFactory.getLogger(TestHashTable.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterAll
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHashTable(TestInfo testInfo) throws Exception {
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    final byte[] family = Bytes.toBytes("family");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] column3 = Bytes.toBytes("c3");

    int numRows = 100;
    int numRegions = 10;
    int numHashFiles = 3;

    byte[][] splitRows = new byte[numRegions - 1][];
    for (int i = 1; i < numRegions; i++) {
      splitRows[i - 1] = Bytes.toBytes(numRows * i / numRegions);
    }

    long timestamp = 1430764183454L;
    // put rows into the first table
    Table t1 = TEST_UTIL.createTable(tableName, family, splitRows);
    for (int i = 0; i < numRows; i++) {
      Put p = new Put(Bytes.toBytes(i), timestamp);
      p.addColumn(family, column1, column1);
      p.addColumn(family, column2, column2);
      p.addColumn(family, column3, column3);
      t1.put(p);
    }
    t1.close();

    HashTable hashTable = new HashTable(TEST_UTIL.getConfiguration());

    Path testDir = TEST_UTIL.getDataTestDirOnTestFS(tableName.getNameAsString());

    long batchSize = 300;
    int code =
      hashTable.run(new String[] { "--batchsize=" + batchSize, "--numhashfiles=" + numHashFiles,
        "--scanbatch=2", tableName.getNameAsString(), testDir.toString() });
    assertEquals(0, code, "test job failed");

    FileSystem fs = TEST_UTIL.getTestFileSystem();

    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testDir);
    assertEquals(tableName.getNameAsString(), tableHash.tableName);
    assertEquals(batchSize, tableHash.batchSize);
    assertEquals(numHashFiles, tableHash.numHashFiles);
    assertEquals(numHashFiles - 1, tableHash.partitions.size());
    for (ImmutableBytesWritable bytes : tableHash.partitions) {
      LOG.debug("partition: " + Bytes.toInt(bytes.get()));
    }

    ImmutableMap<Integer, ImmutableBytesWritable> expectedHashes =
      ImmutableMap.<Integer, ImmutableBytesWritable> builder()
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

    Map<Integer, ImmutableBytesWritable> actualHashes = new HashMap<>();
    Path dataDir = new Path(testDir, HashTable.HASH_DATA_DIR);
    for (int i = 0; i < numHashFiles; i++) {
      Path hashPath = new Path(dataDir, HashTable.TableHash.getDataFileName(i));

      MapFile.Reader reader = new MapFile.Reader(hashPath, fs.getConf());
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      ImmutableBytesWritable hash = new ImmutableBytesWritable();
      while (reader.next(key, hash)) {
        String keyString = Bytes.toHex(key.get(), key.getOffset(), key.getLength());
        LOG.debug("Key: " + (keyString.isEmpty() ? "-1" : Integer.parseInt(keyString, 16))
          + " Hash: " + Bytes.toHex(hash.get(), hash.getOffset(), hash.getLength()));

        int intKey = -1;
        if (key.getLength() > 0) {
          intKey = Bytes.toInt(key.get(), key.getOffset(), key.getLength());
        }
        if (actualHashes.containsKey(intKey)) {
          fail("duplicate key in data files: " + intKey);
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
    assertEquals(expectedHashes, actualHashes);

    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.cleanupDataTestDirOnTestFS();
  }

  @Test
  public void testHashTableWithSha256(TestInfo testInfo) throws Exception {
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    final byte[] family = Bytes.toBytes("family");
    final byte[] column1 = Bytes.toBytes("c1");
    final byte[] column2 = Bytes.toBytes("c2");
    final byte[] column3 = Bytes.toBytes("c3");

    int numRows = 100;
    int numRegions = 10;
    int numHashFiles = 3;

    byte[][] splitRows = new byte[numRegions - 1][];
    for (int i = 1; i < numRegions; i++) {
      splitRows[i - 1] = Bytes.toBytes(numRows * i / numRegions);
    }

    long timestamp = 1430764183454L;
    Table t1 = TEST_UTIL.createTable(tableName, family, splitRows);
    for (int i = 0; i < numRows; i++) {
      Put p = new Put(Bytes.toBytes(i), timestamp);
      p.addColumn(family, column1, column1);
      p.addColumn(family, column2, column2);
      p.addColumn(family, column3, column3);
      t1.put(p);
    }
    t1.close();

    HashTable hashTable = new HashTable(TEST_UTIL.getConfiguration());
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS(tableName.getNameAsString());

    long batchSize = 300;
    int code = hashTable.run(
      new String[] { "--batchsize=" + batchSize, "--numhashfiles=" + numHashFiles, "--scanbatch=2",
        "--hashAlgorithm=SHA-256", tableName.getNameAsString(), testDir.toString() });
    assertEquals(0, code, "test job failed");

    FileSystem fs = TEST_UTIL.getTestFileSystem();
    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testDir);
    assertEquals("SHA-256", tableHash.hashAlgorithm,
      "manifest must record the algorithm used to produce the digests");

    ImmutableMap<Integer, ImmutableBytesWritable> expectedHashes =
      ImmutableMap.<Integer, ImmutableBytesWritable> builder()
        .put(-1,
          new ImmutableBytesWritable(
            Bytes.fromHex("e1452041ec73beb9b5677c0b74ed73a9118ca502d9b2b9abe62ba18d92fc51be")))
        .put(5,
          new ImmutableBytesWritable(
            Bytes.fromHex("6c55999354be6571a8912b7781d37359e88153173cb5fcf143211682d7ac06c5")))
        .put(10,
          new ImmutableBytesWritable(
            Bytes.fromHex("44e9c6aedd838e9a9c78d1983ce5139fda3ef3b0b8a3812893512e7c6f05c51f")))
        .put(15,
          new ImmutableBytesWritable(
            Bytes.fromHex("57f18d831a22057155fb9cfb9bf4706a1c2cf134fc1e92262c8748ca545b58a1")))
        .put(20,
          new ImmutableBytesWritable(
            Bytes.fromHex("83119dfb3deec8901f69cccac31c1039c624870de0cff4b85946147359966d42")))
        .put(25,
          new ImmutableBytesWritable(
            Bytes.fromHex("933eabcc837b7e7b24be2b553b1ec50fb00e95cbf3d8f898f59f5f8bbace0a60")))
        .put(30,
          new ImmutableBytesWritable(
            Bytes.fromHex("b6a3752581d74f362f64b59d56a96ad52763b3245dd5bfc85f6fe9261f2d03f1")))
        .put(35,
          new ImmutableBytesWritable(
            Bytes.fromHex("d3784bac940584dbc0754eff73bc39cce4f9c4aec87939747fff4b0ecc6a0617")))
        .put(40,
          new ImmutableBytesWritable(
            Bytes.fromHex("87f4b810b751abd64e9c22cb7b40b5ce600965e4b8eda2c0eae075d5623088c2")))
        .put(45,
          new ImmutableBytesWritable(
            Bytes.fromHex("ce1f422fcdbe0f926e10b68cb3ead497066560235a1341d29151a9e1847deaab")))
        .put(50,
          new ImmutableBytesWritable(
            Bytes.fromHex("118c771b1eeabe8523f1ad96fb5bf16537d76e0b3855d84c3dbac864de726229")))
        .put(55,
          new ImmutableBytesWritable(
            Bytes.fromHex("00dfe840a275aca3de9268ea61699881a441d47fea93071bca69c39bf7845dac")))
        .put(60,
          new ImmutableBytesWritable(
            Bytes.fromHex("062239ede0306fd9046eb5a3a2f66d997b37c8c1a4defc35789644e66930fff1")))
        .put(65,
          new ImmutableBytesWritable(
            Bytes.fromHex("09a63a94681e75edf975f9b46fe94f1e592840a627cac728a77728b7f9f695aa")))
        .put(70,
          new ImmutableBytesWritable(
            Bytes.fromHex("e634097804d269cbaeef49ce7a009a1388e6f636700badcab05fe20759f6043f")))
        .put(75,
          new ImmutableBytesWritable(
            Bytes.fromHex("69f614ccc16a9c651538681525be1b2e40859c9833a55d9009d77ef39abaffcd")))
        .put(80,
          new ImmutableBytesWritable(
            Bytes.fromHex("6530b957c8064fc043620bee89647960de0d27a0f986b40f183f5347093a12d2")))
        .put(85,
          new ImmutableBytesWritable(
            Bytes.fromHex("403ed0417cd8ab955cbd4c8fe84218cd152b95da9237300050e9b7c90c809faf")))
        .put(90,
          new ImmutableBytesWritable(
            Bytes.fromHex("e27fb9193ae3363fec70a148e62df7c57d514dd7de74a6a332fbda002af67efb")))
        .put(95,
          new ImmutableBytesWritable(
            Bytes.fromHex("a31cb9d55e37f17c773a6eee757f15d6d7fe52d77cd1037fcb7ee00ed2bef6c9")))
        .build();

    Map<Integer, ImmutableBytesWritable> actualHashes = new HashMap<>();
    Path dataDir = new Path(testDir, HashTable.HASH_DATA_DIR);
    for (int i = 0; i < numHashFiles; i++) {
      Path hashPath = new Path(dataDir, HashTable.TableHash.getDataFileName(i));
      try (MapFile.Reader reader = new MapFile.Reader(hashPath, fs.getConf())) {
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable hash = new ImmutableBytesWritable();
        while (reader.next(key, hash)) {
          int intKey = -1;
          if (key.getLength() > 0) {
            intKey = Bytes.toInt(key.get(), key.getOffset(), key.getLength());
          }
          if (actualHashes.containsKey(intKey)) {
            fail("duplicate key in data files: " + intKey);
          }
          actualHashes.put(intKey, new ImmutableBytesWritable(hash.copyBytes()));
        }
      }
    }

    if (!expectedHashes.equals(actualHashes)) {
      LOG.error("Diff: " + Maps.difference(expectedHashes, actualHashes));
    }
    assertEquals(expectedHashes, actualHashes);

    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.cleanupDataTestDirOnTestFS();
  }

  /**
   * A manifest written by an older HashTable does not carry the hashAlgorithm property. Reading
   * such a manifest must default to MD5 so existing on-disk hash data stays usable.
   */
  @Test
  public void testManifestWithoutAlgorithmDefaultsToMd5(TestInfo testInfo) throws Exception {
    Path testDir =
      TEST_UTIL.getDataTestDirOnTestFS(testInfo.getTestMethod().get().getName() + "_legacy");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    fs.mkdirs(testDir);

    // hand-craft a legacy manifest with no hashAlgorithm property
    Properties p = new Properties();
    p.setProperty("table", "legacy");
    p.setProperty("targetBatchSize", "8000");
    p.setProperty("numHashFiles", "1");
    p.setProperty("rawScan", "false");
    Path manifest = new Path(testDir, HashTable.MANIFEST_FILE_NAME);
    try (OutputStream out = fs.create(manifest)) {
      p.store(out, null);
    }

    // write an empty partitions file so TableHash.read() succeeds
    HashTable.TableHash empty = new HashTable.TableHash();
    empty.partitions = new java.util.ArrayList<>();
    empty.writePartitionFile(fs.getConf(), new Path(testDir, HashTable.PARTITIONS_FILE_NAME));

    HashTable.TableHash tableHash = HashTable.TableHash.read(fs.getConf(), testDir);
    assertEquals("MD5", tableHash.hashAlgorithm,
      "Manifests without an algorithm property must default to MD5 for back-compat");

    TEST_UTIL.cleanupDataTestDirOnTestFS();
  }
}
