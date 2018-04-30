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
package org.apache.hadoop.hbase.util;

import static junit.framework.TestCase.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
* Basic test for some old functionality we don't seem to have used but that looks nice.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestRetryCounter {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRetryCounter.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestRetryCounter.class);

  @Test
  public void testBasics() throws InterruptedException {
    int maxAttempts = 10;
    RetryCounterFactory factory =
        new RetryCounterFactory(maxAttempts, 10, 1000);
    RetryCounter retryCounter = factory.create();
    while (retryCounter.shouldRetry()) {
      LOG.info("Attempt={}, backoffTime={}", retryCounter.getAttemptTimes(),
          retryCounter.getBackoffTime());
      retryCounter.sleepUntilNextRetry();
    }
    assertTrue(retryCounter.getAttemptTimes() == maxAttempts);
  }
}
