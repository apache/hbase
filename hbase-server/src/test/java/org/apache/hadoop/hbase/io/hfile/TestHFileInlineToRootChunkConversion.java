/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test a case when an inline index chunk is converted to a root one. This reproduces the bug in
 * HBASE-6871. We write a carefully selected number of relatively large keys so that we accumulate
 * a leaf index chunk that only goes over the configured index chunk size after adding the last
 * key/value. The bug is in that when we close the file, we convert that inline (leaf-level) chunk
 * into a root chunk, but then look at the size of that root chunk, find that it is greater than
 * the configured chunk size, and split it into a number of intermediate index blocks that should
 * really be leaf-level blocks. If more keys were added, we would flush the leaf-level block, add
 * another entry to the root-level block, and that would prevent us from upgrading the leaf-level
 * chunk to the root chunk, thus not triggering the bug. 
 */
@Category({IOTests.class, SmallTests.class})
public class TestHFileInlineToRootChunkConversion {
  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private final Configuration conf = testUtil.getConfiguration();
  
  @Test
  public void testWriteHFile() throws Exception {
    Path hfPath = new Path(testUtil.getDataTestDir(),
        TestHFileInlineToRootChunkConversion.class.getSimpleName() + ".hfile");
    int maxChunkSize = 1024;
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, maxChunkSize); 
    HFileContext context = new HFileContextBuilder().withBlockSize(16).build();
    HFile.Writer hfw = new HFile.WriterFactory(conf, cacheConf)
            .withFileContext(context)
            .withPath(fs, hfPath).create();
    List<byte[]> keys = new ArrayList<byte[]>();
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < 4; ++i) {
      sb.append("key" + String.format("%05d", i));
      sb.append("_");
      for (int j = 0; j < 100; ++j) {
        sb.append('0' + j);
      }
      String keyStr = sb.toString();
      sb.setLength(0);

      byte[] k = Bytes.toBytes(keyStr);
      keys.add(k);
      byte[] v = Bytes.toBytes("value" + i);
      hfw.append(CellUtil.createCell(k, v));
    }
    hfw.close();

    HFile.Reader reader = HFile.createReader(fs, hfPath, cacheConf, conf);
    // Scanner doesn't do Cells yet.  Fix.
    HFileScanner scanner = reader.getScanner(true, true);
    for (int i = 0; i < keys.size(); ++i) {
      scanner.seekTo(CellUtil.createCell(keys.get(i)));
    }
    reader.close();
  }
}