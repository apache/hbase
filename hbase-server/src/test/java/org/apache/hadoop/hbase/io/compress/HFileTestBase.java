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
package org.apache.hadoop.hbase.io.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.RedundantKVGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HFileTestBase {

  protected static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected static final Logger LOG = LoggerFactory.getLogger(HFileTestBase.class);
  protected static FileSystem FS;

  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Disable block cache in this test.
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    FS = FileSystem.get(conf);
  }

  @SuppressWarnings("deprecation")
  public void doTest(Configuration conf, Path path, Compression.Algorithm compression)
      throws Exception {
    // Create 10000 random test KVs
    RedundantKVGenerator generator = new RedundantKVGenerator();
    List<KeyValue> testKvs = generator.generateTestKeyValues(10000);

    // Iterate through data block encoding and compression combinations
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext fileContext = new HFileContextBuilder()
      .withBlockSize(4096) // small block
      .withCompression(compression)
      .build();
    // write a new test HFile
    LOG.info("Writing with " + fileContext);
    FSDataOutputStream out = FS.create(path);
    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
      .withOutputStream(out)
      .withFileContext(fileContext)
      .create();
    try {
      for (KeyValue kv: testKvs) {
        writer.append(kv);
      }
    } finally {
      writer.close();
      out.close();
    }

    // read it back in
    LOG.info("Reading with " + fileContext);
    int i = 0;
    HFileScanner scanner = null;
    HFile.Reader reader = HFile.createReader(FS, path, cacheConf, true, conf);
    try {
      scanner = reader.getScanner(conf, false, false);
      assertTrue("Initial seekTo failed", scanner.seekTo());
      do {
        Cell kv = scanner.getCell();
        assertTrue("Read back an unexpected or invalid KV",
          testKvs.contains(KeyValueUtil.ensureKeyValue(kv)));
        i++;
      } while (scanner.next());
    } finally {
      reader.close();
      scanner.close();
    }

    assertEquals("Did not read back as many KVs as written", i, testKvs.size());

    // Test random seeks with pread
    Random rand = ThreadLocalRandom.current();
    LOG.info("Random seeking with " + fileContext);
    reader = HFile.createReader(FS, path, cacheConf, true, conf);
    try {
      scanner = reader.getScanner(conf, false, true);
      assertTrue("Initial seekTo failed", scanner.seekTo());
      for (i = 0; i < 100; i++) {
        KeyValue kv = testKvs.get(rand.nextInt(testKvs.size()));
        assertEquals("Unable to find KV as expected: " + kv, 0, scanner.seekTo(kv));
      }
    } finally {
      scanner.close();
      reader.close();
    }
  }

}
