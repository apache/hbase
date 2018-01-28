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
package org.apache.hadoop.hbase.io.hadoopbackport;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestThrottledInputStream {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThrottledInputStream.class);

  @Test
  public void testCalSleepTimeMs() {
    // case 0: initial - no read, no sleep
    assertEquals(0, ThrottledInputStream.calSleepTimeMs(0, 10000, 1000));

    // case 1: no threshold
    assertEquals(0, ThrottledInputStream.calSleepTimeMs(Long.MAX_VALUE, 0, 1000));
    assertEquals(0, ThrottledInputStream.calSleepTimeMs(Long.MAX_VALUE, -1, 1000));

    // case 2: too fast
    assertEquals(1500, ThrottledInputStream.calSleepTimeMs(5, 2, 1000));
    assertEquals(500, ThrottledInputStream.calSleepTimeMs(5, 2, 2000));
    assertEquals(6500, ThrottledInputStream.calSleepTimeMs(15, 2, 1000));

    // case 3: too slow
    assertEquals(0, ThrottledInputStream.calSleepTimeMs(1, 2, 1000));
    assertEquals(0, ThrottledInputStream.calSleepTimeMs(2, 2, 2000));
    assertEquals(0, ThrottledInputStream.calSleepTimeMs(1, 2, 1000));
  }

}
