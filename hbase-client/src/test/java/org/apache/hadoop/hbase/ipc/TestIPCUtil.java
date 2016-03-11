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

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.io.SizedCellScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestIPCUtil {

  private static final Log LOG = LogFactory.getLog(TestIPCUtil.class);

  IPCUtil util;
  @Before
  public void before() {
    this.util = new IPCUtil(new Configuration());
  }

  @Test
  public void testBuildCellBlock() throws IOException {
    doBuildCellBlockUndoCellBlock(this.util, new KeyValueCodec(), null);
    doBuildCellBlockUndoCellBlock(this.util, new KeyValueCodec(), new DefaultCodec());
    doBuildCellBlockUndoCellBlock(this.util, new KeyValueCodec(), new GzipCodec());
  }

  static void doBuildCellBlockUndoCellBlock(final IPCUtil util,
      final Codec codec, final CompressionCodec compressor)
  throws IOException {
    doBuildCellBlockUndoCellBlock(util, codec, compressor, 10, 1, false);
  }

  static void doBuildCellBlockUndoCellBlock(final IPCUtil util, final Codec codec,
    final CompressionCodec compressor, final int count, final int size, final boolean sized)
  throws IOException {
    Cell [] cells = getCells(count, size);
    CellScanner cellScanner = sized? getSizedCellScanner(cells):
      CellUtil.createCellScanner(Arrays.asList(cells).iterator());
    ByteBuffer bb = util.buildCellBlock(codec, compressor, cellScanner);
    cellScanner = util.createCellScannerReusingBuffers(codec, compressor, bb);
    int i = 0;
    while (cellScanner.advance()) {
      i++;
    }
    assertEquals(count, i);
  }

  static CellScanner getSizedCellScanner(final Cell [] cells) {
    int size = -1;
    for (Cell cell: cells) {
      size += CellUtil.estimatedSerializedSizeOf(cell);
    }
    final int totalSize = ClassSize.align(size);
    final CellScanner cellScanner = CellUtil.createCellScanner(cells);
    return new SizedCellScanner() {
      @Override
      public long heapSize() {
        return totalSize;
      }

      @Override
      public Cell current() {
        return cellScanner.current();
      }

      @Override
      public boolean advance() throws IOException {
        return cellScanner.advance();
      }
    };
  }

  static Cell [] getCells(final int howMany) {
    return getCells(howMany, 1024);
  }

  static Cell [] getCells(final int howMany, final int valueSize) {
    Cell [] cells = new Cell[howMany];
    byte [] value = new byte[valueSize];
    for (int i = 0; i < howMany; i++) {
      byte [] index = Bytes.toBytes(i);
      KeyValue kv = new KeyValue(index, Bytes.toBytes("f"), index, value);
      cells[i] = kv;
    }
    return cells;
  }

  private static final String COUNT = "--count=";
  private static final String SIZE = "--size=";

  /**
   * Prints usage and then exits w/ passed <code>errCode</code>
   * @param errCode
   */
  private static void usage(final int errCode) {
    System.out.println("Usage: IPCUtil [options]");
    System.out.println("Micro-benchmarking how changed sizes and counts work with buffer resizing");
    System.out.println(" --count  Count of Cells");
    System.out.println(" --size   Size of Cell values");
    System.out.println("Example: IPCUtil --count=1024 --size=1024");
    System.exit(errCode);
  }

  private static void timerTests(final IPCUtil util, final int count, final int size,
      final Codec codec, final CompressionCodec compressor)
  throws IOException {
    final int cycles = 1000;
    StopWatch timer = new StopWatch();
    timer.start();
    for (int i = 0; i < cycles; i++) {
      timerTest(util, timer, count, size, codec, compressor, false);
    }
    timer.stop();
    LOG.info("Codec=" + codec + ", compression=" + compressor + ", sized=" + false +
        ", count=" + count + ", size=" + size + ", + took=" + timer.getTime() + "ms");
    timer.reset();
    timer.start();
    for (int i = 0; i < cycles; i++) {
      timerTest(util, timer, count, size, codec, compressor, true);
    }
    timer.stop();
    LOG.info("Codec=" + codec + ", compression=" + compressor + ", sized=" + true +
      ", count=" + count + ", size=" + size + ", + took=" + timer.getTime() + "ms");
  }

  private static void timerTest(final IPCUtil util, final StopWatch timer, final int count,
      final int size, final Codec codec, final CompressionCodec compressor, final boolean sized)
  throws IOException {
    doBuildCellBlockUndoCellBlock(util, codec, compressor, count, size, sized);
  }

  /**
   * For running a few tests of methods herein.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    int count = 1024;
    int size = 10240;
    for (String arg: args) {
      if (arg.startsWith(COUNT)) {
        count = Integer.parseInt(arg.replace(COUNT, ""));
      } else if (arg.startsWith(SIZE)) {
        size = Integer.parseInt(arg.replace(SIZE, ""));
      } else {
        usage(1);
      }
    }
    IPCUtil util = new IPCUtil(HBaseConfiguration.create());
    ((Log4JLogger)IPCUtil.LOG).getLogger().setLevel(Level.ALL);
    timerTests(util, count, size,  new KeyValueCodec(), null);
    timerTests(util, count, size,  new KeyValueCodec(), new DefaultCodec());
    timerTests(util, count, size,  new KeyValueCodec(), new GzipCodec());
  }
}
