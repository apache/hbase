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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.BloomFilterMetrics;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.MultiTenantBloomSupport;
import org.apache.hadoop.hbase.io.hfile.MultiTenantHFileWriter;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Test;

public class TestMultiTenantBloomFilterDelegation {

  @Test
  public void testRowBloomDelegation() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    conf.setBoolean(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, true);
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);

    FileSystem fs = FileSystem.getLocal(conf);
    Path baseDir = new Path(Files.createTempDirectory("multi-tenant-bloom").toUri());
    Path file = StoreFileWriter.getUniqueFile(fs, baseDir);

    CacheConfig cacheConfig = new CacheConfig(conf);

    Map<String, String> tableProps = new HashMap<>();
    tableProps.put(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
    tableProps.put(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, "2");
    tableProps.put("BLOOMFILTER", "ROW");

    HFileContext context = new HFileContextBuilder().withBlockSize(4096)
      .withColumnFamily(Bytes.toBytes("cf")).withTableName(Bytes.toBytes("tbl")).build();

    MultiTenantHFileWriter writer = MultiTenantHFileWriter.create(fs, file, conf, cacheConfig,
      tableProps, context, BloomType.ROW, BloomType.ROW, null, true);

    long ts = EnvironmentEdgeManager.currentTime();
    KeyValue tenantOneRow = new KeyValue(Bytes.toBytes("aa-0001"), Bytes.toBytes("cf"),
      Bytes.toBytes("q"), ts, Bytes.toBytes("value"));
    KeyValue tenantTwoRow = new KeyValue(Bytes.toBytes("bb-0001"), Bytes.toBytes("cf"),
      Bytes.toBytes("q"), ts, Bytes.toBytes("value"));

    writer.append(tenantOneRow);
    writer.append(tenantTwoRow);
    writer.close();

    ReaderContext contextReader =
      new ReaderContextBuilder().withFileSystemAndPath(fs, file).build();
    StoreFileInfo storeFileInfo = StoreFileInfo.createStoreFileInfoForHFile(conf, fs, file, true);
    storeFileInfo.initHFileInfo(contextReader);
    StoreFileReader reader = storeFileInfo.createReader(contextReader, cacheConfig);
    storeFileInfo.getHFileInfo().initMetaAndIndex(reader.getHFileReader());
    reader.loadFileInfo();
    reader.loadBloomfilter(BlockType.GENERAL_BLOOM_META, new BloomFilterMetrics());
    reader.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META, new BloomFilterMetrics());

    byte[] presentRow = Bytes.toBytes("bb-0001");
    byte[] absentRow = Bytes.toBytes("bb-zzzz");

    HFile.Reader hfileReader = reader.getHFileReader();
    assertTrue(hfileReader instanceof MultiTenantBloomSupport);
    MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) hfileReader;

    boolean expectedPresent = bloomSupport.passesGeneralRowBloomFilter(presentRow, 0,
      presentRow.length);
    assertTrue(expectedPresent);
    Scan present = new Scan(new Get(presentRow));
    assertEquals(expectedPresent, reader.passesBloomFilter(present, null));

    boolean expectedAbsent = bloomSupport.passesGeneralRowBloomFilter(absentRow, 0,
      absentRow.length);
    Scan absent = new Scan(new Get(absentRow));
    assertEquals(expectedAbsent, reader.passesBloomFilter(absent, null));

    fs.delete(baseDir, true);
  }
}
