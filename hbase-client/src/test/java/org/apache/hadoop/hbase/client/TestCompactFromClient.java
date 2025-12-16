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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestCompactFromClient {

  private org.apache.logging.log4j.core.Appender mockAppender;

  @BeforeEach
  public void setUp() {
    mockAppender = mock(org.apache.logging.log4j.core.Appender.class);
    when(mockAppender.getName()).thenReturn("mockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
  }

  @AfterEach
  public void tearDown() {
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(FutureUtils.class)).removeAppender(mockAppender);
  }

  @Test
  public void testCompactTableWithNullLocations() throws Exception {
    AtomicReference<String> msg = new AtomicReference<>();
    AtomicReference<Throwable> throwable = new AtomicReference<>();

    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(FutureUtils.class)).addAppender(mockAppender);
    doAnswer(invocation -> {
      org.apache.logging.log4j.core.LogEvent logEvent =
        invocation.getArgument(0, org.apache.logging.log4j.core.LogEvent.class);
      org.apache.logging.log4j.Level originalLevel = logEvent.getLevel();
      if (originalLevel.equals(org.apache.logging.log4j.Level.ERROR)) {
        msg.set(logEvent.getMessage().getFormattedMessage());
        throwable.set(logEvent.getThrown());
      }
      return null;
    }).when(mockAppender).append(any(org.apache.logging.log4j.core.LogEvent.class));

    TableName tableName = TableName.valueOf("testCompactNullLocations");
    CompletableFuture<List<HRegionLocation>> nullLocationsFuture =
      CompletableFuture.completedFuture(null);

    try (
      MockedStatic<ClientMetaTableAccessor> mockedMeta = mockStatic(ClientMetaTableAccessor.class);
      AsyncConnectionImpl connection = mock(AsyncConnectionImpl.class)) {
      mockedMeta.when(() -> ClientMetaTableAccessor.getTableHRegionLocations(any(AsyncTable.class),
        any(TableName.class))).thenReturn(nullLocationsFuture);
      AsyncTable<AdvancedScanResultConsumer> metaTable = mock(AsyncTable.class);
      when(connection.getTable(MetaTableName.getInstance())).thenReturn(metaTable);

      HashedWheelTimer hashedWheelTimer = mock(HashedWheelTimer.class);
      AsyncAdminBuilderBase asyncAdminBuilderBase = mock(AsyncAdminBuilderBase.class);
      RawAsyncHBaseAdmin admin =
        new RawAsyncHBaseAdmin(connection, hashedWheelTimer, asyncAdminBuilderBase);

      CompletableFuture<Void> future = admin.compact(tableName, CompactType.NORMAL);
      // future.get() throws ExecutionException, and the cause is TableNotFoundException
      ExecutionException ex =
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
      assertInstanceOf(TableNotFoundException.class, ex.getCause(),
        "Expected TableNotFoundException as the cause of ExecutionException");

      assertNull(msg.get(), "No error message should be logged");
      assertNull(throwable.get(), "No Exception should be logged");
    }
  }
}
