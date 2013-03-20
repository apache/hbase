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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class) 
public class TestIPCUtil {
  IPCUtil util;
  @Before
  public void before() {
    this.util = new IPCUtil(new Configuration());
  }
  
  @Test
  public void testBuildCellBlock() throws IOException {
    doBuildCellBlockUndoCellBlock(new KeyValueCodec(), null);
    doBuildCellBlockUndoCellBlock(new KeyValueCodec(), new DefaultCodec());
    doBuildCellBlockUndoCellBlock(new KeyValueCodec(), new GzipCodec());
  }

  void doBuildCellBlockUndoCellBlock(final Codec codec, final CompressionCodec compressor)
  throws IOException {
    final int count = 10;
    Cell [] cells = getCells(count);
    ByteBuffer bb = this.util.buildCellBlock(codec, compressor,
      CellUtil.createCellScanner(Arrays.asList(cells).iterator()));
    CellScanner scanner =
      this.util.createCellScanner(codec, compressor, bb.array(), 0, bb.limit());
    int i = 0;
    while (scanner.advance()) {
      i++;
    }
    assertEquals(count, i);
  }

  static Cell [] getCells(final int howMany) {
    Cell [] cells = new Cell[howMany];
    for (int i = 0; i < howMany; i++) {
      byte [] index = Bytes.toBytes(i);
      KeyValue kv = new KeyValue(index, Bytes.toBytes("f"), index, index);
      cells[i] = kv;
    }
    return cells;
  }
}