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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.PerformanceEvaluation.RandomReadTest;
import org.apache.hadoop.hbase.PerformanceEvaluation.TestOptions;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.gson.Gson;

@Category({MiscTests.class, SmallTests.class})
public class TestPerformanceEvaluation {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPerformanceEvaluation.class);

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  @Test
  public void testDefaultInMemoryCompaction() {
    PerformanceEvaluation.TestOptions defaultOpts =
        new PerformanceEvaluation.TestOptions();
    assertEquals(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_DEFAULT,
        defaultOpts.getInMemoryCompaction().toString());
    TableDescriptor tableDescriptor = PerformanceEvaluation.getTableDescriptor(defaultOpts);
    for (ColumnFamilyDescriptor familyDescriptor : tableDescriptor.getColumnFamilies()) {
      assertEquals(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_DEFAULT,
        familyDescriptor.getInMemoryCompaction().toString());
    }
  }

  @Test
  public void testSerialization() {
    PerformanceEvaluation.TestOptions options = new PerformanceEvaluation.TestOptions();
    assertFalse(options.isAutoFlush());
    options.setAutoFlush(true);
    Gson gson = GsonUtil.createGson().create();
    String optionsString = gson.toJson(options);
    PerformanceEvaluation.TestOptions optionsDeserialized =
      gson.fromJson(optionsString, PerformanceEvaluation.TestOptions.class);
    assertTrue(optionsDeserialized.isAutoFlush());
  }

  /**
   * Exercise the mr spec writing. Simple assertions to make sure it is basically working.
   */
  @Test
  public void testWriteInputFile() throws IOException {
    TestOptions opts = new PerformanceEvaluation.TestOptions();
    final int clients = 10;
    opts.setNumClientThreads(clients);
    opts.setPerClientRunRows(10);
    Path dir =
      PerformanceEvaluation.writeInputFile(HTU.getConfiguration(), opts, HTU.getDataTestDir());
    FileSystem fs = FileSystem.get(HTU.getConfiguration());
    Path p = new Path(dir, PerformanceEvaluation.JOB_INPUT_FILENAME);
    long len = fs.getFileStatus(p).getLen();
    assertTrue(len > 0);
    byte[] content = new byte[(int) len];
    try (FSDataInputStream dis = fs.open(p)) {
      dis.readFully(content);
      BufferedReader br = new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8));
      int count = 0;
      while (br.readLine() != null) {
        count++;
      }
      assertEquals(clients, count);
    }
  }

  @Test
  public void testSizeCalculation() {
    TestOptions opts = new PerformanceEvaluation.TestOptions();
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    int rows = opts.getPerClientRunRows();
    // Default row count
    final int defaultPerClientRunRows = 1024 * 1024;
    assertEquals(defaultPerClientRunRows, rows);
    // If size is 2G, then twice the row count.
    opts.setSize(2.0f);
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    assertEquals(defaultPerClientRunRows * 2, opts.getPerClientRunRows());
    // If two clients, then they get half the rows each.
    opts.setNumClientThreads(2);
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    assertEquals(defaultPerClientRunRows, opts.getPerClientRunRows());
    // What if valueSize is 'random'? Then half of the valueSize so twice the rows.
    opts.valueRandom = true;
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    assertEquals(defaultPerClientRunRows * 2, opts.getPerClientRunRows());
  }

  @Test
  public void testRandomReadCalculation() {
    TestOptions opts = new PerformanceEvaluation.TestOptions();
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    int rows = opts.getPerClientRunRows();
    // Default row count
    final int defaultPerClientRunRows = 1024 * 1024;
    assertEquals(defaultPerClientRunRows, rows);
    // If size is 2G, then twice the row count.
    opts.setSize(2.0f);
    opts.setPerClientRunRows(1000);
    opts.setCmdName(PerformanceEvaluation.RANDOM_READ);
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    assertEquals(1000, opts.getPerClientRunRows());
    // If two clients, then they get half the rows each.
    opts.setNumClientThreads(2);
    opts = PerformanceEvaluation.calculateRowsAndSize(opts);
    assertEquals(1000, opts.getPerClientRunRows());
    // assuming we will get one before this loop expires
    boolean foundValue = false;
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 10000000; i++) {
      int randomRow = PerformanceEvaluation.generateRandomRow(rand, opts.totalRows);
      if (randomRow > 1000) {
        foundValue = true;
        break;
      }
    }
    assertTrue("We need to get a value more than 1000", foundValue);
  }

  @Test
  public void testZipfian() throws NoSuchMethodException, SecurityException, InstantiationException,
      IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    TestOptions opts = new PerformanceEvaluation.TestOptions();
    opts.setValueZipf(true);
    final int valueSize = 1024;
    opts.setValueSize(valueSize);
    RandomReadTest rrt = new RandomReadTest(null, opts, null);
    Constructor<?> ctor =
      Histogram.class.getDeclaredConstructor(com.codahale.metrics.Reservoir.class);
    ctor.setAccessible(true);
    Histogram histogram = (Histogram)ctor.newInstance(new UniformReservoir(1024 * 500));
    for (int i = 0; i < 100; i++) {
      histogram.update(rrt.getValueLength(null));
    }
    Snapshot snapshot = histogram.getSnapshot();
    double stddev = snapshot.getStdDev();
    assertTrue(stddev != 0 && stddev != 1.0);
    assertTrue(snapshot.getStdDev() != 0);
    double median = snapshot.getMedian();
    assertTrue(median != 0 && median != 1 && median != valueSize);
  }

  @Test
  public void testSetBufferSizeOption() {
    TestOptions opts = new PerformanceEvaluation.TestOptions();
    long bufferSize = opts.getBufferSize();
    assertEquals(bufferSize, 2L * 1024L * 1024L);
    opts.setBufferSize(64L * 1024L);
    bufferSize = opts.getBufferSize();
    assertEquals(bufferSize, 64L * 1024L);
  }

  @Test
  public void testParseOptsWithThreads() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    int threads = 1;
    opts.offer(cmdName);
    opts.offer(String.valueOf(threads));
    PerformanceEvaluation.TestOptions options = PerformanceEvaluation.parseOpts(opts);
    assertNotNull(options);
    assertNotNull(options.getCmdName());
    assertEquals(cmdName, options.getCmdName());
    assertEquals(threads, options.getNumClientThreads());
  }

  @Test
  public void testParseOptsWrongThreads() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    opts.offer(cmdName);
    opts.offer("qq");
    try {
      PerformanceEvaluation.parseOpts(opts);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      assertEquals("Command " + cmdName + " does not have threads number", e.getMessage());
      assertTrue(e.getCause() instanceof NumberFormatException);
    }
  }

  @Test
  public void testParseOptsNoThreads() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    try {
      PerformanceEvaluation.parseOpts(opts);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      assertEquals("Command " + cmdName + " does not have threads number", e.getMessage());
      assertTrue(e.getCause() instanceof NoSuchElementException);
    }
  }

  @Test
  public void testParseOptsMultiPuts() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    opts.offer("--multiPut=10");
    opts.offer(cmdName);
    opts.offer("64");
    PerformanceEvaluation.TestOptions options = null;
    try {
      options = PerformanceEvaluation.parseOpts(opts);
      fail("should fail");
    } catch (IllegalArgumentException  e) {
      System.out.println(e.getMessage());
    }

    //Re-create options
    opts = new LinkedList<>();
    opts.offer("--autoFlush=true");
    opts.offer("--multiPut=10");
    opts.offer(cmdName);
    opts.offer("64");

    options = PerformanceEvaluation.parseOpts(opts);
    assertNotNull(options);
    assertNotNull(options.getCmdName());
    assertEquals(cmdName, options.getCmdName());
    assertEquals(10, options.getMultiPut());
  }

  @Test
  public void testParseOptsMultiPutsAndAutoFlushOrder() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    String cmdMultiPut = "--multiPut=10";
    String cmdAutoFlush = "--autoFlush=true";
    opts.offer(cmdAutoFlush);
    opts.offer(cmdMultiPut);
    opts.offer(cmdName);
    opts.offer("64");
    PerformanceEvaluation.TestOptions options = null;
    options = PerformanceEvaluation.parseOpts(opts);
    assertNotNull(options);
    assertEquals(true, options.autoFlush);
    assertEquals(10, options.getMultiPut());

    // Change the order of AutoFlush and Multiput
    opts = new LinkedList<>();
    opts.offer(cmdMultiPut);
    opts.offer(cmdAutoFlush);
    opts.offer(cmdName);
    opts.offer("64");

    options = null;
    options = PerformanceEvaluation.parseOpts(opts);
    assertNotNull(options);
    assertEquals(10, options.getMultiPut());
    assertEquals(true, options.autoFlush);
  }

  @Test
  public void testParseOptsConnCount() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    opts.offer("--oneCon=true");
    opts.offer("--connCount=10");
    opts.offer(cmdName);
    opts.offer("64");
    PerformanceEvaluation.TestOptions options = null;
    try {
      options = PerformanceEvaluation.parseOpts(opts);
      fail("should fail");
    } catch (IllegalArgumentException  e) {
      System.out.println(e.getMessage());
    }

    opts = new LinkedList<>();
    opts.offer("--connCount=10");
    opts.offer(cmdName);
    opts.offer("64");

    options = PerformanceEvaluation.parseOpts(opts);
    assertNotNull(options);
    assertNotNull(options.getCmdName());
    assertEquals(cmdName, options.getCmdName());
    assertEquals(10, options.getConnCount());
  }

  @Test
  public void testParseOptsValueRandom() {
    Queue<String> opts = new LinkedList<>();
    String cmdName = "sequentialWrite";
    opts.offer("--valueRandom");
    opts.offer("--valueZipf");
    opts.offer(cmdName);
    opts.offer("64");
    PerformanceEvaluation.TestOptions options = null;
    try {
      options = PerformanceEvaluation.parseOpts(opts);
      fail("should fail");
    } catch (IllegalStateException  e) {
      System.out.println(e.getMessage());
    }

    opts = new LinkedList<>();
    opts.offer("--valueRandom");
    opts.offer(cmdName);
    opts.offer("64");

    options = PerformanceEvaluation.parseOpts(opts);

    assertNotNull(options);
    assertNotNull(options.getCmdName());
    assertEquals(cmdName, options.getCmdName());
    assertEquals(true, options.valueRandom);
  }

}
