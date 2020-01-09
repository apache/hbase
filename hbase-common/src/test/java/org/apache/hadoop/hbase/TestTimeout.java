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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTimeout {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeout.class);

  @Test
  public void run1() throws InterruptedException {
    Thread.sleep(100);
  }

  /**
   * Enable to check if timeout works.
   * Can't enable as it waits 30seconds and expected doesn't do Exception catching
   */
  @Ignore
  @Test
  public void infiniteLoop() {
    // Launch a background non-daemon thread.
    Thread t = new Thread("HangingThread") {
      public void run() {
        synchronized(this) {
          while (true) {}
        }
      }
    };
    t.start();
    while (true) {}
  }
}
