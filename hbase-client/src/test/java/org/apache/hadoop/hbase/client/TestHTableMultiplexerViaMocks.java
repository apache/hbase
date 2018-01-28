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
package org.apache.hadoop.hbase.client;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHTableMultiplexerViaMocks {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHTableMultiplexerViaMocks.class);

  private HTableMultiplexer mockMultiplexer;
  private ClusterConnection mockConnection;

  @Before
  public void setupTest() {
    mockMultiplexer = mock(HTableMultiplexer.class);
    mockConnection = mock(ClusterConnection.class);

    // Call the real put(TableName, Put, int) method
    when(mockMultiplexer.put(any(TableName.class), any(), anyInt())).thenCallRealMethod();

    // Return the mocked ClusterConnection
    when(mockMultiplexer.getConnection()).thenReturn(mockConnection);
  }

  @SuppressWarnings("deprecation")
  @Test public void testConnectionClosing() throws IOException {
    doCallRealMethod().when(mockMultiplexer).close();
    // If the connection is not closed
    when(mockConnection.isClosed()).thenReturn(false);

    mockMultiplexer.close();

    // We should close it
    verify(mockConnection).close();
  }

  @SuppressWarnings("deprecation")
  @Test public void testClosingAlreadyClosedConnection() throws IOException {
    doCallRealMethod().when(mockMultiplexer).close();
    // If the connection is already closed
    when(mockConnection.isClosed()).thenReturn(true);

    mockMultiplexer.close();

    // We should not close it again
    verify(mockConnection, times(0)).close();
  }
}
