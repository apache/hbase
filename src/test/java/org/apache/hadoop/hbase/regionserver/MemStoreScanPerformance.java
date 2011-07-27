/*
 * Copyright 2011 The Apache Software Foundation
 *
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

import org.apache.hadoop.hbase.KeyValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class MemStoreScanPerformance {
  private MemStore m;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testTenColumns() {
    ReadWriteConsistencyControl.resetThreadReadPoint();
    long totalScanTime = 0;
    long totalRows = 0;
    long totalLoadTime = 0;

    for (int size = 50000; size < 160000; size += 25000) {
      m = new MemStore();
      long start = System.currentTimeMillis();
      for (int i = 0; i < size; i++) {
        byte[] row = format(i);
        for (int j = 0; j < 10; j++) {
          byte[] column = format(j);
          this.m.add(new KeyValue(row, column, column));
        }
      }

      KeyValue searched = new KeyValue(format(5), format(5), format(5));

      totalLoadTime += (System.currentTimeMillis() - start);
      System.out.println("Loaded in " + (System.currentTimeMillis() - start) +
        " ms");
      for (int i = 0; i < 10; ++i) {
        try {
          start = System.currentTimeMillis();
          KeyValueScanner scanner = this.m.getScanners().get(0);

          scanner.seek(searched);

          while (true) {
            KeyValue kv = scanner.next();
            if (kv == null) break;
            kv = scanner.peek();
          }
          totalScanTime += (System.currentTimeMillis() - start);
          totalRows += size;
          System.out.println("Scan with size " + size + ": " +
            (System.currentTimeMillis() - start) + " ms");
        } catch (IOException e) {
          throw new Error(e);
        }
      }
    }
    System.out.println("Total load time: " + totalLoadTime + " ms (i.e:" +
      (totalLoadTime / 1000L) + " seconds)");
    System.out.println("Total scan time: " + totalScanTime + " ms (i.e:" +
      (totalScanTime / 1000L) + " seconds)");
    System.out.println("Rows scanned per seconds: " +
      ((totalRows * 1000) / totalScanTime));
    System.out.println("Rows loaded per seconds: " +
      ((totalRows * 1000) / totalLoadTime));
  }

  private static byte[] format(final int number) {
    byte[] b = new byte[10];
    int d = Math.abs(number);
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte) ((d % 10) + '0');
      d /= 10;
    }

    return b;
  }

  public static void main(String[] args) {
    org.junit.runner.JUnitCore.main(MemStoreScanPerformance.class.getName());
  }
}
