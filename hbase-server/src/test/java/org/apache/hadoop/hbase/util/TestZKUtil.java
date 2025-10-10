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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestZKUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestZKUtil.class);

  private ZKWatcher zkWatcher;

  @Before
  public void setUp() throws Exception {
    zkWatcher = mock(ZKWatcher.class);
  }

  @Test
  public void testFormatZKStringNullQuorum() {
    when(zkWatcher.getQuorum()).thenReturn(null);
    String result = ZKUtil.formatZKString(zkWatcher);
    assertEquals("", result);
  }

  @Test
  public void testFormatZKStringEmptyQuorum() {
    when(zkWatcher.getQuorum()).thenReturn("");
    String result = ZKUtil.formatZKString(zkWatcher);
    assertEquals("", result);
  }

  @Test
  public void testFormatZKStringOneServer() {
    when(zkWatcher.getQuorum()).thenReturn("127.0.0.1:21818");
    String result = ZKUtil.formatZKString(zkWatcher);
    assertEquals("127.0.0.1:21818", result);
  }

  @Test
  public void testFormatZKStringMultipleServers() {
    when(zkWatcher.getQuorum()).thenReturn("127.0.0.1:21818,127.0.0.1:21819,127.0.0.1:21820");
    String result = ZKUtil.formatZKString(zkWatcher);
    assertEquals("127.0.0.1:21818<br/>127.0.0.1:21819<br/>127.0.0.1:21820", result);
  }
}
