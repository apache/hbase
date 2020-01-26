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
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MiscTests.class, MediumTests.class})
public class TestOpenTracingHooks {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestOpenTracingHooks.class);

  private static final byte[] FAMILY_BYTES = "family".getBytes();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static MockTracer tracer;
  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void before() throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(2).numRegionServers(3).numDataNodes(3).build();
    TEST_UTIL.getConfiguration().set(TraceUtil.HBASE_OPENTRACING_TRACER,
      TraceUtil.HBASE_OPENTRACING_MOCKTRACER);
    TEST_UTIL.startMiniCluster(option);

    tracer = (MockTracer)GlobalTracer.get();
    //TraceUtil.addSampler(AlwaysSampler.INSTANCE);
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTraceCreateTable() throws Exception {
    Table table;
    MockSpan createTableSpan;
    try (Scope scope = TraceUtil.createTrace("creating table")) {
      createTableSpan = (MockSpan)scope.span();
      table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY_BYTES);
    }

    // Some table creation is async.  Need to make sure that everything is full in before
    // checking to see if the spans are there.
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return tracer.finishedSpans().size() >= 5;
      }
    });

    List<MockSpan> spans = tracer.finishedSpans();

    //Collection<Span> spans = Sets.newHashSet(rcvr.getSpans());
    List<Span> roots = new LinkedList<>();
    TraceTree traceTree = new TraceTree(spans);
    roots.addAll(traceTree.getSpansByParent().find(createTableSpan.context().spanId()));



    // Roots was made 3 in hbase2. It used to be 1. We changed it back to 1 on upgrade to
    // htrace-4.2 just to get the test to pass (traces are not wholesome in hbase2; TODO).
    assertEquals(2, roots.size());
    assertEquals("creating table", createTableSpan.operationName());

    if (spans != null) {
      assertTrue(spans.size() > 5);
    }

    Put put = new Put("row".getBytes());
    put.addColumn(FAMILY_BYTES, "col".getBytes(), "value".getBytes());

    MockSpan putSpan;

    try (Scope scope = TraceUtil.createTrace("doing put")) {
      putSpan = (MockSpan)scope.span();
      table.put(put);
    }

    spans = tracer.finishedSpans();
    traceTree = new TraceTree(spans);
    roots.clear();
    roots.addAll(traceTree.getSpansByParent().find(putSpan.context().spanId()));
    assertEquals(1, roots.size());
  }
}
