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
package org.apache.hadoop.hbase.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.TraceTree;
import org.apache.htrace.impl.POJOSpanReceiver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({MiscTests.class, MediumTests.class})
public class TestHTraceHooks {

  private static final byte[] FAMILY_BYTES = "family".getBytes();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static POJOSpanReceiver rcvr;
  private static long ROOT_SPAN_ID = 0;

  @BeforeClass
  public static void before() throws Exception {

    // Find out what the right value to use fo SPAN_ROOT_ID after HTRACE-111. We use HTRACE-32
    // to find out to detect if we are using HTrace 3.2 or not.
    try {
        Method m = Span.class.getMethod("addKVAnnotation", String.class, String.class);
    } catch (NoSuchMethodException e) {
      ROOT_SPAN_ID = 0x74aceL; // Span.SPAN_ROOT_ID pre HTrace-3.2
    }

    TEST_UTIL.startMiniCluster(2, 3);
    rcvr = new POJOSpanReceiver(new HBaseHTraceConfiguration(TEST_UTIL.getConfiguration()));
    Trace.addReceiver(rcvr);
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    Trace.removeReceiver(rcvr);
    rcvr = null;
  }

  @Test
  public void testTraceCreateTable() throws Exception {
    TraceScope tableCreationSpan = Trace.startSpan("creating table", Sampler.ALWAYS);
    Table table;
    try {

      table = TEST_UTIL.createTable(TableName.valueOf("table"),
        FAMILY_BYTES);
    } finally {
      tableCreationSpan.close();
    }

    // Some table creation is async.  Need to make sure that everything is full in before
    // checking to see if the spans are there.
    TEST_UTIL.waitFor(1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return rcvr.getSpans().size() >= 5;
      }
    });

    Collection<Span> spans = rcvr.getSpans();
    TraceTree traceTree = new TraceTree(spans);
    Collection<Span> roots = traceTree.getSpansByParent().find(ROOT_SPAN_ID);

    assertEquals(1, roots.size());
    Span createTableRoot = roots.iterator().next();

    assertEquals("creating table", createTableRoot.getDescription());

    int createTableCount = 0;

    for (Span s : traceTree.getSpansByParent().find(createTableRoot.getSpanId())) {
      if (s.getDescription().startsWith("MasterService.CreateTable")) {
        createTableCount++;
      }
    }

    assertTrue(createTableCount >= 1);
    assertTrue(traceTree.getSpansByParent().find(createTableRoot.getSpanId()).size() > 3);
    assertTrue(spans.size() > 5);
    
    Put put = new Put("row".getBytes());
    put.add(FAMILY_BYTES, "col".getBytes(), "value".getBytes());

    TraceScope putSpan = Trace.startSpan("doing put", Sampler.ALWAYS);
    try {
      table.put(put);
    } finally {
      putSpan.close();
    }

    spans = rcvr.getSpans();
    traceTree = new TraceTree(spans);
    roots = traceTree.getSpansByParent().find(ROOT_SPAN_ID);

    assertEquals(2, roots.size());
    Span putRoot = null;
    for (Span root : roots) {
      if (root.getDescription().equals("doing put")) {
        putRoot = root;
      }
    }
    
    assertNotNull(putRoot);
  }
}
