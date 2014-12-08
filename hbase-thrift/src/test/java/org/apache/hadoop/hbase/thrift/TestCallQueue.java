/*
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.thrift;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.thrift.CallQueue.Call;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;

/**
 * Unit testing for CallQueue, a part of the
 * org.apache.hadoop.hbase.thrift package.
 */
@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestCallQueue {

  public static final Log LOG = LogFactory.getLog(TestCallQueue.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final MetricsAssertHelper metricsHelper =
      CompatibilitySingletonFactory.getInstance(MetricsAssertHelper.class);

  private int elementsAdded;
  private int elementsRemoved;

  @Parameters
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();
    for (int elementsAdded : new int[] {100, 200, 300}) {
      for (int elementsRemoved : new int[] {0, 20, 100}) {
        parameters.add(new Object[]{new Integer(elementsAdded),
                                    new Integer(elementsRemoved)});
      }
    }
    return parameters;
  }

  public TestCallQueue(int elementsAdded, int elementsRemoved) {
    this.elementsAdded = elementsAdded;
    this.elementsRemoved = elementsRemoved;
    LOG.debug("elementsAdded:" + elementsAdded +
              " elementsRemoved:" + elementsRemoved);

  }

  @Test(timeout = 60000)
  public void testPutTake() throws Exception {
    ThriftMetrics metrics = createMetrics();
    CallQueue callQueue = new CallQueue(
        new LinkedBlockingQueue<Call>(), metrics);
    for (int i = 0; i < elementsAdded; ++i) {
      callQueue.put(createDummyRunnable());
    }
    for (int i = 0; i < elementsRemoved; ++i) {
      callQueue.take();
    }
    verifyMetrics(metrics, "timeInQueue_num_ops", elementsRemoved);
  }

  @Test(timeout = 60000)
  public void testOfferPoll() throws Exception {
    ThriftMetrics metrics = createMetrics();
    CallQueue callQueue = new CallQueue(
        new LinkedBlockingQueue<Call>(), metrics);
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

