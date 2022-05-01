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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.apache.hadoop.hbase.regionserver.compactions.CloseChecker.SIZE_LIMIT_KEY;
import static org.apache.hadoop.hbase.regionserver.compactions.CloseChecker.TIME_LIMIT_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCloseChecker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCloseChecker.class);

  @Test
  public void testIsClosed() {
    Store enableWrite = mock(Store.class);
    when(enableWrite.areWritesEnabled()).thenReturn(true);

    Store disableWrite = mock(Store.class);
    when(disableWrite.areWritesEnabled()).thenReturn(false);

    Configuration conf = new Configuration();

    long currentTime = System.currentTimeMillis();

    conf.setInt(SIZE_LIMIT_KEY, 10);
    conf.setLong(TIME_LIMIT_KEY, 10);

    CloseChecker closeChecker = new CloseChecker(conf, currentTime);
    assertFalse(closeChecker.isTimeLimit(enableWrite, currentTime));
    assertFalse(closeChecker.isSizeLimit(enableWrite, 10L));

    closeChecker = new CloseChecker(conf, currentTime);
    assertFalse(closeChecker.isTimeLimit(enableWrite, currentTime + 11));
    assertFalse(closeChecker.isSizeLimit(enableWrite, 11L));

    closeChecker = new CloseChecker(conf, currentTime);
    assertTrue(closeChecker.isTimeLimit(disableWrite, currentTime + 11));
    assertTrue(closeChecker.isSizeLimit(disableWrite, 11L));

    for (int i = 0; i < 10; i++) {
      int plusTime = 5 * i;
      assertFalse(closeChecker.isTimeLimit(enableWrite, currentTime + plusTime));
      assertFalse(closeChecker.isSizeLimit(enableWrite, 5L));
    }

    closeChecker = new CloseChecker(conf, currentTime);
    assertFalse(closeChecker.isTimeLimit(disableWrite, currentTime + 6));
    assertFalse(closeChecker.isSizeLimit(disableWrite, 6));
    assertTrue(closeChecker.isTimeLimit(disableWrite, currentTime + 12));
    assertTrue(closeChecker.isSizeLimit(disableWrite, 6));
  }
}
