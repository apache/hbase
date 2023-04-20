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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestRpcThrottlingException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcThrottlingException.class);

  @Test
  public void itHandlesSecWaitIntervalMessage() {
    try {
      RpcThrottlingException.throwNumReadRequestsExceeded(1001);
    } catch (RpcThrottlingException e) {
      assertTrue(e.getMessage().contains("wait 1sec, 1ms"));
    }
  }

  @Test
  public void itHandlesMsWaitIntervalMessage() {
    try {
      RpcThrottlingException.throwNumReadRequestsExceeded(50);
    } catch (RpcThrottlingException e) {
      assertTrue(e.getMessage().contains("wait 50ms"));
    }
  }

  @Test
  public void itHandlesMinWaitIntervalMessage() {
    try {
      RpcThrottlingException.throwNumReadRequestsExceeded(65_015);
    } catch (RpcThrottlingException e) {
      assertTrue(e.getMessage().contains("wait 1mins, 5sec, 15ms"));
    }
  }

}
