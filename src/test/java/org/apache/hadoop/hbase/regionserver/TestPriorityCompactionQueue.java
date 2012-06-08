/*
 * Copyright 2010 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the priority compaction queue
 */
public class TestPriorityCompactionQueue {
  static final Log LOG = LogFactory.getLog(TestPriorityCompactionQueue.class);

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {

  }

  static class DummyHRegion extends HRegion {
    String name;

    DummyHRegion(Configuration conf, String name,
        Path tableDir, FileSystem fs, HRegionInfo regionInfo) {
      super(tableDir, null, fs, conf, regionInfo, null);
      this.name = name;
    }

    static DummyHRegion createRegion(
        Configuration conf, String name) throws IOException {
      byte[] startKey = Bytes.toBytes(name + "-A-startKey");
      byte[] endKey = Bytes.toBytes(name + "-B-endKey");
      HTableDescriptor htd = new HTableDescriptor(name);
      HRegionInfo regionInfo = new HRegionInfo(htd, startKey, endKey);
      Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir, htd.getNameAsString());
      return new DummyHRegion(conf, name, p, fs, regionInfo);
    }

    public int hashCode() {
      return name.hashCode();
    }

    public boolean equals(DummyHRegion r) {
      return name.equals(r.name);
    }

    public String toString() {
      return "[DummyHRegion " + name + "]";
    }

    public String getRegionNameAsString() {
      return name;
    }
  }

  protected void getAndCheckRegion(PriorityCompactionQueue pq,
      HRegion checkRegion) {
    HRegion r = pq.remove();
    if (r != checkRegion) {
      Assert.assertTrue("Didn't get expected " + checkRegion + " got " + r, r
          .equals(checkRegion));
    }
  }

  protected void addRegion(PriorityCompactionQueue pq, HRegion r, int p) {
    pq.add(r, p);
    try {
      // Sleep 1 millisecond so 2 things are not put in the queue within the
      // same millisecond. The queue breaks ties arbitrarily between two
      // requests inserted at the same time. We want the ordering to
      // be consistent for our unit test.
      Thread.sleep(1);
    } catch (InterruptedException ex) {
      // continue
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // tests
  // ////////////////////////////////////////////////////////////////////////////

  /** tests general functionality of the compaction queue */
  @Test public void testPriorityQueue()
      throws IOException, InterruptedException {
    PriorityCompactionQueue pq = new PriorityCompactionQueue();

    Configuration conf = HBaseConfiguration.create();
    HRegion r1 = DummyHRegion.createRegion(conf, "r1");
    HRegion r2 = DummyHRegion.createRegion(conf, "r2");
    HRegion r3 = DummyHRegion.createRegion(conf, "r3");
    HRegion r4 = DummyHRegion.createRegion(conf, "r4");
    HRegion r5 = DummyHRegion.createRegion(conf, "r5");

    // test 1
    // check fifo w/priority
    addRegion(pq, r1, 0);
    addRegion(pq, r2, 0);
    addRegion(pq, r3, 0);
    addRegion(pq, r4, 0);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r5);

    // test 2
    // check fifo w/mixed priority
    addRegion(pq, r1, 0);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, 0);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r5);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r4);

    // test 3
    // check fifo w/mixed priority
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r5);
    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);

    // test 4
    // check fifo w/mixed priority elevation time
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, 0);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    Thread.sleep(1000);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, 0);

    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r5);
    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);

    // reset the priority compaction queue back to a normal queue
    pq = new PriorityCompactionQueue();

    // test 5
    // test that lower priority are removed from the queue when a high priority
    // is added
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, 0);

    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r5);

    Assert.assertTrue("Queue should be empty.", pq.size() == 0);

    // test 6
    // don't add the same region more than once
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r3, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r4, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r5, CompactSplitThread.PRIORITY_USER);

    getAndCheckRegion(pq, r1);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r5);

    Assert.assertTrue("Queue should be empty.", pq.size() == 0);
    
    // test 7
    // we can handle negative priorities
    addRegion(pq, r1, CompactSplitThread.PRIORITY_USER);
    addRegion(pq, r2, -1);
    addRegion(pq, r3, 0);    
    addRegion(pq, r4, -2);
    
    getAndCheckRegion(pq, r4);
    getAndCheckRegion(pq, r2);
    getAndCheckRegion(pq, r3);
    getAndCheckRegion(pq, r1);
    
    Assert.assertTrue("Queue should be empty.", pq.size() == 0);
  }
}
