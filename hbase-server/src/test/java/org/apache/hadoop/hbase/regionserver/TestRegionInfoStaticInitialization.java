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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for the tangled mess that is static initialization of our our {@link HRegionInfo} and
 * {@link RegionInfoBuilder}, as reported on HBASE-24896. The condition being tested can only be
 * reproduced the first time a JVM loads the classes under test. Thus, this test is marked as a
 * {@link LargeTests} because, under their current configuration, tests in that category are run
 * in their own JVM instances.
 */
@SuppressWarnings("deprecation")
@Category({ RegionServerTests.class, LargeTests.class})
public class TestRegionInfoStaticInitialization {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionInfoStaticInitialization.class);

  @Test
  public void testParallelStaticInitialization() throws Exception {
    // The JVM loads symbols lazily. These suppliers reference two symbols that, before this patch,
    // are mutually dependent and expose a deadlock in the loading of symbols from RegionInfo and
    // RegionInfoBuilder.
    final Supplier<RegionInfo> retrieveUNDEFINED = () -> HRegionInfo.UNDEFINED;
    final Supplier<RegionInfo> retrieveMetaRegionInfo =
      () -> RegionInfoBuilder.FIRST_META_REGIONINFO;

    // The test runs multiple threads that reference these mutually dependent symbols. In order to
    // express this bug, these threads need to access these symbols at roughly the same time, so
    // that the classloader is asked to materialize these symbols concurrently. These Suppliers are
    // run on threads that have already been allocated, managed by the system's ForkJoin pool.
    final CompletableFuture<?>[] futures = Stream.of(
      retrieveUNDEFINED, retrieveMetaRegionInfo, retrieveUNDEFINED, retrieveMetaRegionInfo)
      .map(CompletableFuture::supplyAsync)
      .toArray(CompletableFuture<?>[]::new);

    // Loading classes should be relatively fast. 5 seconds is an arbitrary choice of timeout. It
    // was chosen under the assumption that loading these symbols should complete much faster than
    // this window.
    CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);
  }
}
