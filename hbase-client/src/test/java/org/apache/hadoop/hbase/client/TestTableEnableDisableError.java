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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;

@Category({ ClientTests.class, SmallTests.class })
public class TestTableEnableDisableError {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableEnableDisableError.class);

  private MockedStatic<AsyncMetaTableAccessor> mockedClientMetaTableAccessor;

  private AsyncAdmin admin;

  @Before
  public void setUp() {
    mockedClientMetaTableAccessor = mockStatic(AsyncMetaTableAccessor.class);
    AsyncConnectionImpl conn = mock(AsyncConnectionImpl.class);
    AsyncAdminBuilderBase builder =
      new AsyncAdminBuilderBase(new AsyncConnectionConfiguration(HBaseConfiguration.create())) {

        @Override
        public AsyncAdmin build() {
          return new RawAsyncHBaseAdmin(conn, null, this);
        }
      };
    admin = builder.build();
  }

  @After
  public void tearDown() {
    mockedClientMetaTableAccessor.closeOnDemand();
  }

  @Test
  public void testIsTableEnabledError() {
    HBaseIOException expected = new HBaseIOException("expected");
    mockedClientMetaTableAccessor.when(() -> AsyncMetaTableAccessor.getTableState(any(), any()))
      .thenReturn(FutureUtils.failedFuture(expected));
    HBaseIOException actual = assertThrows(HBaseIOException.class,
      () -> FutureUtils.get(admin.isTableEnabled(TableName.valueOf("whatever"))));
    assertSame(expected, actual);
  }

  @Test
  public void testIsTableDisabledError() {
    HBaseIOException expected = new HBaseIOException("expected");
    mockedClientMetaTableAccessor.when(() -> AsyncMetaTableAccessor.getTableState(any(), any()))
      .thenReturn(FutureUtils.failedFuture(expected));
    HBaseIOException actual = assertThrows(HBaseIOException.class,
      () -> FutureUtils.get(admin.isTableDisabled(TableName.valueOf("whatever"))));
    assertSame(expected, actual);
  }
}
