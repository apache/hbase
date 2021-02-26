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
package org.apache.hadoop.hbase.zookeeper;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ZKTests.class, SmallTests.class })
public class TestInstancePending {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestInstancePending.class);

  @Test
  public void test() throws Exception {
    final InstancePending<String> pending = new InstancePending<>();
    final AtomicReference<String> getResultRef = new AtomicReference<>();

    new Thread() {
      @Override
      public void run() {
        getResultRef.set(pending.get());
      }
    }.start();

    Thread.sleep(100);
    Assert.assertNull(getResultRef.get());

    pending.prepare("abc");
    Thread.sleep(100);
    Assert.assertEquals("abc", getResultRef.get());
  }
}
