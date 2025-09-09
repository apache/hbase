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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRefreshHFilesBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshHFilesBase.class);

  protected static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected Admin admin;
  protected ProcedureExecutor<MasterProcedureEnv> procExecutor;
  protected static Configuration conf;
  protected static final TableName TEST_TABLE = TableName.valueOf("testRefreshHFilesTable");
  protected static final String TEST_NAMESPACE = "testRefreshHFilesNamespace";
  protected static final byte[] TEST_FAMILY = Bytes.toBytes("testRefreshHFilesCF1");

  protected void createTableAndWait(TableName table, byte[] cf)
    throws IOException, InterruptedException {
    TEST_UTIL.createTable(table, cf);
    TEST_UTIL.waitTableAvailable(table);
  }

  protected void createTableInNamespaceAndWait(String namespace, TableName table, byte[] cf)
    throws IOException, InterruptedException {
    TableName fqTableName = TableName.valueOf(namespace + table.getNameAsString());
    TEST_UTIL.createTable(fqTableName, cf);
    TEST_UTIL.waitTableAvailable(fqTableName);
  }

  protected void deleteTable(TableName table) throws IOException {
    TEST_UTIL.deleteTableIfAny(table);
  }

  protected void createNamespace(String namespace) throws RuntimeException {
    try {
      final NamespaceDescriptor nsd = NamespaceDescriptor.create(namespace).build();
      // Create the namespace if it doesn’t exist
      if (
        Arrays.stream(admin.listNamespaceDescriptors())
          .noneMatch(ns -> ns.getName().equals(namespace))
      ) {
        admin.createNamespace(nsd);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void deleteNamespace(String namespace) {
    try {
      // List table in namespace
      TableName[] tables = admin.listTableNamesByNamespace(namespace);
      for (TableName t : tables) {
        TEST_UTIL.deleteTableIfAny(t);
      }
      // Now delete the namespace
      admin.deleteNamespace(namespace);
    } catch (Exception e) {
      LOG.debug(
        "Unable to delete namespace " + namespace + " post test execution. This isn't a failure");
    }
  }

  @Before
  public void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Shorten the run time of failed unit tests by limiting retries and the session timeout
    // threshold
    conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);

    try {
      // Start the test cluster
      TEST_UTIL.startMiniCluster(1);
      admin = TEST_UTIL.getAdmin();
      procExecutor = TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    } catch (Exception e) {
      TEST_UTIL.shutdownMiniCluster();
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (admin != null) {
      admin.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

}
