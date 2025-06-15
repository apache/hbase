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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.io.SizedExtendedCellScanner;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, SmallTests.class })
public class TestCellBlockBuilder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCellBlockBuilder.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCellBlockBuilder.class);

  private CellBlockBuilder builder;

  @Before
  public void before() {
    this.builder = new CellBlockBuilder(HBaseConfiguration.create());
  }

  @Test
  public void testBuildCellBlock() throws IOException {
    doBuildCellBlockUndoCellBlock(this.builder, new KeyValueCodec(), null);
    doBuildCellBlockUndoCellBlock(this.builder, new KeyValueCodec(), new DefaultCodec());
    doBuildCellBlockUndoCellBlock(this.builder, new KeyValueCodec(), new GzipCodec());
  }

  static void doBuildCellBlockUndoCellBlock(final CellBlockBuilder builder, final Codec codec,
    final CompressionCodec compressor) throws IOException {
    doBuildCellBlockUndoCellBlock(builder, codec, compressor, 10, 1, false);
  }

  static void doBuildCellBlockUndoCellBlock(final CellBlockBuilder builder, final Codec codec,
    final CompressionCodec compressor, final int count, final int size, final boolean sized)
    throws IOException {
    ExtendedCell[] cells = getCells(count, size);
    ExtendedCellScanner cellScanner =
      sized ? getSizedCellScanner(cells) : PrivateCellUtil.createExtendedCellScanner(cells);
    ByteBuffer bb = builder.buildCellBlock(codec, compressor, cellScanner);
    cellScanner =
      builder.createCellScannerReusingBuffers(codec, compressor, new SingleByteBuff(bb));
    int i = 0;
    while (cellScanner.advance()) {
      i++;
    }
    assertEquals(count, i);
  }

  static ExtendedCellScanner getSizedCellScanner(final ExtendedCell[] cells) {
    int size = -1;
    for (Cell cell : cells) {
      size += PrivateCellUtil.estimatedSerializedSizeOf(cell);
    }
    final int totalSize = ClassSize.align(size);
    final ExtendedCellScanner cellScanner = PrivateCellUtil.createExtendedCellScanner(cells);
    return new SizedExtendedCellScanner() {
      @Override
      public long heapSize() {
        return totalSize;
      }

      @Override
      public ExtendedCell current() {
        return cellScanner.current();
      }

      @Override
      public boolean advance() throws IOException {
        return cellScanner.advance();
      }
    };
  }

  static ExtendedCell[] getCells(final int howMany) {
    return getCells(howMany, 1024);
  }

  static ExtendedCell[] getCells(final int howMany, final int valueSize) {
    ExtendedCell[] cells = new ExtendedCell[howMany];
    byte[] value = new byte[valueSize];
    for (int i = 0; i < howMany; i++) {
      byte[] index = Bytes.toBytes(i);
      KeyValue kv = new KeyValue(index, Bytes.toBytes("f"), index, value);
      cells[i] = kv;
    }
    return cells;
  }

  private static final String COUNT = "--count=";
  private static final String SIZE = "--size=";

  /**
   * Prints usage and then exits w/ passed <code>errCode</code>
   * @param errorCode the error code to use to exit the application
   */
  private static void usage(final int errorCode) {
    System.out.println("Usage: IPCUtil [options]");
    System.out.println("Micro-benchmarking how changed sizes and counts work with buffer resizing");
    System.out.println(" --count  Count of Cells");
    System.out.println(" --size   Size of Cell values");
    System.out.println("Example: IPCUtil --count=1024 --size=1024");
    ExitHandler.getInstance().exit(errorCode);
  }

  private static void timerTests(final CellBlockBuilder builder, final int count, final int size,
    final Codec codec, final CompressionCodec compressor) throws IOException {
    final int cycles = 1000;
    StopWatch timer = new StopWatch();
    timer.start();
    for (int i = 0; i < cycles; i++) {
      timerTest(builder, count, size, codec, compressor, false);
    }
    timer.stop();
    LOG.info("Codec=" + codec + ", compression=" + compressor + ", sized=" + false + ", count="
      + count + ", size=" + size + ", + took=" + timer.getTime() + "ms");
    timer.reset();
    timer.start();
    for (int i = 0; i < cycles; i++) {
      timerTest(builder, count, size, codec, compressor, true);
    }
    timer.stop();
    LOG.info("Codec=" + codec + ", compression=" + compressor + ", sized=" + true + ", count="
      + count + ", size=" + size + ", + took=" + timer.getTime() + "ms");
  }

  private static void timerTest(final CellBlockBuilder builder, final int count, final int size,
    final Codec codec, final CompressionCodec compressor, final boolean sized) throws IOException {
    doBuildCellBlockUndoCellBlock(builder, codec, compressor, count, size, sized);
  }

  /**
   * For running a few tests of methods herein.
   * @param args the arguments to use for the timer test
   * @throws IOException if creating the build fails
   */
  public static void main(String[] args) throws IOException {
    int count = 1024;
    int size = 10240;
    for (String arg : args) {
      if (arg.startsWith(COUNT)) {
        count = Integer.parseInt(arg.replace(COUNT, ""));
      } else if (arg.startsWith(SIZE)) {
        size = Integer.parseInt(arg.replace(SIZE, ""));
      } else {
        usage(1);
      }
    }
    CellBlockBuilder builder = new CellBlockBuilder(HBaseConfiguration.create());
    timerTests(builder, count, size, new KeyValueCodec(), null);
    timerTests(builder, count, size, new KeyValueCodec(), new DefaultCodec());
    timerTests(builder, count, size, new KeyValueCodec(), new GzipCodec());
  }
}
