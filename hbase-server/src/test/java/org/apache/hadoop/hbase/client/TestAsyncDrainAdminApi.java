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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncDrainAdminApi extends TestAsyncAdminBase {

  /*
   * This test drains all regions so cannot be run in parallel with other tests.
   */
  @Ignore @Test(timeout = 30000)
  public void testDrainRegionServers() throws Exception {
    List<ServerName> drainingServers = admin.listDrainingRegionServers().get();
    assertTrue(drainingServers.isEmpty());

    // Drain all region servers.
    Collection<ServerName> clusterServers = admin.getRegionServers().get();
    drainingServers = new ArrayList<>();
    for (ServerName server : clusterServers) {
      drainingServers.add(server);
    }
    admin.drainRegionServers(drainingServers).join();

    // Check that drain lists all region servers.
    drainingServers = admin.listDrainingRegionServers().get();
    assertEquals(clusterServers.size(), drainingServers.size());
    for (ServerName server : clusterServers) {
      assertTrue(drainingServers.contains(server));
    }

    // Try for 20 seconds to create table (new region). Will not complete because all RSs draining.
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    final Runnable createTable = new Thread() {
      @Override
      public void run() {
        try {
          admin.createTable(builder.build()).join();
        } catch (Exception ioe) {
          assertTrue(false); // Should not get IOException.
        }
      }
    };

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final java.util.concurrent.Future<?> future = executor.submit(createTable);
    executor.shutdown();
    try {
      future.get(20, TimeUnit.SECONDS);
    } catch (TimeoutException ie) {
      assertTrue(true); // Expecting timeout to happen.
    }

    // Kill executor if still processing.
    if (!executor.isTerminated()) {
      executor.shutdownNow();
      assertTrue(true);
    }

    // Remove drain list.
    admin.removeDrainFromRegionServers(drainingServers);
    drainingServers = admin.listDrainingRegionServers().get();
    assertTrue(drainingServers.isEmpty());
  }
}
