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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

public final class BalancerConditionalsTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerConditionalsTestUtil.class);

  private BalancerConditionalsTestUtil() {
  }

  static byte[][] generateSplits(int numRegions) {
    byte[][] splitKeys = new byte[numRegions - 1][];
    for (int i = 0; i < numRegions - 1; i++) {
      splitKeys[i] =
        Bytes.toBytes(String.format("%09d", (i + 1) * (Integer.MAX_VALUE / numRegions)));
    }
    return splitKeys;
  }

  static void printRegionLocations(Connection connection) throws IOException {
    Admin admin = connection.getAdmin();

    // Get all table names in the cluster
    Set<TableName> tableNames = admin.listTableDescriptors(true).stream()
      .map(TableDescriptor::getTableName).collect(Collectors.toSet());

    // Group regions by server
    Map<ServerName, Map<TableName, List<RegionInfo>>> serverToRegions =
      admin.getClusterMetrics().getLiveServerMetrics().keySet().stream()
        .collect(Collectors.toMap(server -> server, server -> {
          try {
            return listRegionsByTable(connection, server, tableNames);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));

    // Pretty print region locations
    StringBuilder regionLocationOutput = new StringBuilder();
    regionLocationOutput.append("Pretty printing region locations...\n");
    serverToRegions.forEach((server, tableRegions) -> {
      regionLocationOutput.append("Server: " + server.getServerName() + "\n");
      tableRegions.forEach((table, regions) -> {
        if (regions.isEmpty()) {
          return;
        }
        regionLocationOutput.append("  Table: " + table.getNameAsString() + "\n");
        regions.forEach(region -> regionLocationOutput
          .append(String.format("    Region: %s, start: %s, end: %s, replica: %s\n",
            region.getEncodedName(), Bytes.toString(region.getStartKey()),
            Bytes.toString(region.getEndKey()), region.getReplicaId())));
      });
    });
    LOG.info(regionLocationOutput.toString());
  }

  private static Map<TableName, List<RegionInfo>> listRegionsByTable(Connection connection,
    ServerName server, Set<TableName> tableNames) throws IOException {
    Admin admin = connection.getAdmin();

    // Find regions for each table
    return tableNames.stream().collect(Collectors.toMap(tableName -> tableName, tableName -> {
      List<RegionInfo> allRegions = null;
      try {
        allRegions = admin.getRegions(server);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return allRegions.stream().filter(region -> region.getTable().equals(tableName))
        .collect(Collectors.toList());
    }));
  }

  static void validateReplicaDistribution(Connection connection, TableName tableName,
    boolean shouldBeDistributed) {
    Map<ServerName, List<RegionInfo>> serverToRegions = null;
    try {
      serverToRegions = connection.getRegionLocator(tableName).getAllRegionLocations().stream()
        .collect(Collectors.groupingBy(location -> location.getServerName(),
          Collectors.mapping(location -> location.getRegion(), Collectors.toList())));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (shouldBeDistributed) {
      // Ensure no server hosts more than one replica of any region
      for (Map.Entry<ServerName, List<RegionInfo>> serverAndRegions : serverToRegions.entrySet()) {
        List<RegionInfo> regionInfos = serverAndRegions.getValue();
        Set<byte[]> startKeys = new HashSet<>();
        for (RegionInfo regionInfo : regionInfos) {
          // each region should have a distinct start key
          assertFalse(
            "Each region should have its own start key, "
              + "demonstrating it is not a replica of any others on this host",
            startKeys.contains(regionInfo.getStartKey()));
          startKeys.add(regionInfo.getStartKey());
        }
      }
    } else {
      // Ensure all replicas are on the same server
      assertEquals("All regions should share one server", 1, serverToRegions.size());
    }
  }

  static void validateRegionLocations(Map<TableName, Set<ServerName>> tableToServers,
    TableName productTableName, boolean shouldBeBalanced) {
    ServerName metaServer =
      tableToServers.get(MetaTableName.getInstance()).stream().findFirst().orElseThrow();
    ServerName quotaServer =
      tableToServers.get(QuotaUtil.QUOTA_TABLE_NAME).stream().findFirst().orElseThrow();
    Set<ServerName> productServers = tableToServers.get(productTableName);

    if (shouldBeBalanced) {
      for (ServerName server : productServers) {
        assertNotEquals("Meta table and product table should not share servers", server,
          metaServer);
        assertNotEquals("Quota table and product table should not share servers", server,
          quotaServer);
      }
      assertNotEquals("The meta server and quotas server should be different", metaServer,
        quotaServer);
    } else {
      for (ServerName server : productServers) {
        assertEquals("Meta table and product table must share servers", server, metaServer);
        assertEquals("Quota table and product table must share servers", server, quotaServer);
      }
      assertEquals("The meta server and quotas server must be the same", metaServer, quotaServer);
    }
  }

  static Map<TableName, Set<ServerName>> getTableToServers(Connection connection,
    Set<TableName> tableNames) {
    return tableNames.stream().collect(Collectors.toMap(t -> t, t -> {
      try {
        return connection.getRegionLocator(t).getAllRegionLocations().stream()
          .map(HRegionLocation::getServerName).collect(Collectors.toSet());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));
  }

  @FunctionalInterface
  interface AssertionRunnable {
    void run() throws AssertionError;
  }

  static void validateAssertionsWithRetries(HBaseTestingUtil testUtil, boolean runBalancerOnFailure,
    AssertionRunnable assertion) {
    validateAssertionsWithRetries(testUtil, runBalancerOnFailure, ImmutableSet.of(assertion));
  }

  static void validateAssertionsWithRetries(HBaseTestingUtil testUtil, boolean runBalancerOnFailure,
    Set<AssertionRunnable> assertions) {
    int maxAttempts = 50;
    for (int i = 0; i < maxAttempts; i++) {
      try {
        for (AssertionRunnable assertion : assertions) {
          assertion.run();
        }
      } catch (AssertionError e) {
        if (i == maxAttempts - 1) {
          throw e;
        }
        try {
          LOG.warn("Failed to validate region locations. Will retry", e);
          Thread.sleep(1000);
          BalancerConditionalsTestUtil.printRegionLocations(testUtil.getConnection());
          if (runBalancerOnFailure) {
            testUtil.getAdmin().balance();
          }
          Thread.sleep(1000);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

}
