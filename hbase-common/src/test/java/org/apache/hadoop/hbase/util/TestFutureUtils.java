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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestFutureUtils {

  private ExecutorService executor;

  @BeforeEach
  public void setUp() {
    executor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
  }

  @AfterEach
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void testRecordStackTrace() throws IOException {
    CompletableFuture<Void> future = new CompletableFuture<>();
    executor.execute(() -> future.completeExceptionally(new HBaseIOException("Inject error!")));
    HBaseIOException e = assertThrows(HBaseIOException.class, () -> FutureUtils.get(future),
      "The future should have been completed exceptionally");
    assertEquals("Inject error!", e.getMessage());
    StackTraceElement[] elements = e.getStackTrace();
    assertThat(elements[0].toString(), containsString("java.lang.Thread.getStackTrace"));
    assertThat(elements[1].toString(),
      startsWith("org.apache.hadoop.hbase.util.FutureUtils.setStackTrace"));
    assertThat(elements[2].toString(),
      startsWith("org.apache.hadoop.hbase.util.FutureUtils.rethrow"));
    assertThat(elements[3].toString(), startsWith("org.apache.hadoop.hbase.util.FutureUtils.get"));
    assertThat(elements[4].toString(),
      startsWith("org.apache.hadoop.hbase.util.TestFutureUtils.lambda"));
    int i = 5;
    for (;;) {
      if (!elements[i].toString().contains("assertThrows")) {
        break;
      }
      i++;
    }
    assertThat(elements[i].toString(),
      startsWith("org.apache.hadoop.hbase.util.TestFutureUtils.testRecordStackTrace"));
    assertTrue(Stream.of(elements)
      .anyMatch(element -> element.toString().contains("--------Future.get--------")));
  }
}
