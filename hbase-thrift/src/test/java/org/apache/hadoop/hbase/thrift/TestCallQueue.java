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
package org.apache.hadoop.hbase.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit testing for CallQueue, a part of the org.apache.hadoop.hbase.thrift package.
 */
@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate
public class TestCallQueue {

  private static final Logger LOG = LoggerFactory.getLogger(TestCallQueue.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final MetricsAssertHelper metricsHelper =
    CompatibilitySingletonFactory.getInstance(MetricsAssertHelper.class);

  private int elementsAdded;
  private int elementsRemoved;

  public static Stream<Arguments> parameters() {
    List<Arguments> params = new ArrayList<>();
    for (int elementsAdded : new int[] { 100, 200, 300 }) {
      for (int elementsRemoved : new int[] { 0, 20, 100 }) {
        params.add(Arguments.of(elementsAdded, elementsRemoved));
      }
    }
    return params.stream();
  }

  public TestCallQueue(int elementsAdded, int elementsRemoved) {
    this.elementsAdded = elementsAdded;
    this.elementsRemoved = elementsRemoved;
    LOG.debug("elementsAdded:" + elementsAdded + " elementsRemoved:" + elementsRemoved);

  }

  @TestTemplate
  public void testPutTake() throws Exception {
    ThriftMetrics metrics = createMetrics();
    CallQueue callQueue = new CallQueue(new LinkedBlockingQueue<>(), metrics);
    for (int i = 0; i < elementsAdded; ++i) {
      callQueue.put(createDummyRunnable());
    }
    for (int i = 0; i < elementsRemoved; ++i) {
      callQueue.take();
    }
    verifyMetrics(metrics, "timeInQueue_num_ops", elementsRemoved);
  }

  @TestTemplate
  public void testOfferPoll() throws Exception {
    ThriftMetrics metrics = createMetrics();
    CallQueue callQueue = new CallQueue(new LinkedBlockingQueue<>(), metrics);
    for (int i = 0; i < elementsAdded; ++i) {
      callQueue.offer(createDummyRunnable());
    }
    for (int i = 0; i < elementsRemoved; ++i) {
      callQueue.poll();
    }
    verifyMetrics(metrics, "timeInQueue_num_ops", elementsRemoved);
  }

  private static ThriftMetrics createMetrics() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ThriftMetrics m = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.ONE);
    m.getSource().init();
    return m;
  }

  private static void verifyMetrics(ThriftMetrics metrics, String name, int expectValue)
    throws Exception {
    metricsHelper.assertCounter(name, expectValue, metrics.getSource());
  }

  private static Runnable createDummyRunnable() {
    return new Runnable() {
      @Override
      public void run() {
      }
    };
  }
}
