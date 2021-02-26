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

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Integration test that verifies Procedure V2.
 *
 * DDL operations should go through (rollforward or rollback) when primary master is killed by
 * ChaosMonkey (default MASTER_KILLING).
 *
 * <p></p>Multiple Worker threads are started to randomly do the following Actions in loops:
 * Actions generating and populating tables:
 * <ul>
 *     <li>CreateTableAction</li>
 *     <li>DisableTableAction</li>
 *     <li>EnableTableAction</li>
 *     <li>DeleteTableAction</li>
 *     <li>AddRowAction</li>
 * </ul>
 * Actions performing column family DDL operations:
 * <ul>
 *     <li>AddColumnFamilyAction</li>
 *     <li>AlterColumnFamilyVersionsAction</li>
 *     <li>AlterColumnFamilyEncodingAction</li>
 *     <li>DeleteColumnFamilyAction</li>
 * </ul>
 * Actions performing namespace DDL operations:
 * <ul>
 *     <li>AddNamespaceAction</li>
 *     <li>AlterNamespaceAction</li>
 *     <li>DeleteNamespaceAction</li>
 * </ul>
 * <br/>
 *
 * The threads run for a period of time (default 20 minutes) then are stopped at the end of
 * runtime. Verification is performed towards those checkpoints:
 * <ol>
 *     <li>No Actions throw Exceptions.</li>
 *     <li>No inconsistencies are detected in hbck.</li>
 * </ol>
 *
 * <p>
 * This test should be run by the hbase user since it invokes hbck at the end
 * </p><p>
 * Usage:
 *  hbase org.apache.hadoop.hbase.IntegrationTestDDLMasterFailover
 *    -Dhbase.IntegrationTestDDLMasterFailover.runtime=1200000
 *    -Dhbase.IntegrationTestDDLMasterFailover.numThreads=20
 *    -Dhbase.IntegrationTestDDLMasterFailover.numRegions=50 --monkey masterKilling
 */

@Category(IntegrationTests.class)
public class IntegrationTestDDLMasterFailover extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestDDLMasterFailover.class);

  private static final int SERVER_COUNT = 1; // number of slaves for the smallest cluster

  protected static final long DEFAULT_RUN_TIME = 20 * 60 * 1000;

  protected static final int DEFAULT_NUM_THREADS = 20;

  protected static final int DEFAULT_NUM_REGIONS = 50; // number of regions in pre-split tables

  private boolean keepObjectsAtTheEnd = false;
  protected HBaseCluster cluster;

  protected Connection connection;

  /**
   * A soft limit on how long we should run
   */
  protected static final String RUN_TIME_KEY = "hbase.%s.runtime";
  protected static final String NUM_THREADS_KEY = "hbase.%s.numThreads";
  protected static final String NUM_REGIONS_KEY = "hbase.%s.numRegions";

  protected AtomicBoolean running = new AtomicBoolean(true);

  protected AtomicBoolean create_table = new AtomicBoolean(true);

  protected int numThreads, numRegions;

  ConcurrentHashMap<String, NamespaceDescriptor> namespaceMap = new ConcurrentHashMap<>();

  ConcurrentHashMap<TableName, TableDescriptor> enabledTables = new ConcurrentHashMap<>();

  ConcurrentHashMap<TableName, TableDescriptor> disabledTables = new ConcurrentHashMap<>();

  ConcurrentHashMap<TableName, TableDescriptor> deletedTables = new ConcurrentHashMap<>();

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    LOG.debug("Initializing/checking cluster has " + SERVER_COUNT + " servers");
    util.initializeCluster(getMinServerCount());
    LOG.debug("Done initializing/checking cluster");
    cluster = util.getHBaseClusterInterface();
  }

  @Override
  public void cleanUpCluster() throws Exception {
    if (!keepObjectsAtTheEnd) {
      Admin admin = util.getAdmin();
      admin.disableTables("ittable-\\d+");
      admin.deleteTables("ittable-\\d+");
      NamespaceDescriptor [] nsds = admin.listNamespaceDescriptors();
      for(NamespaceDescriptor nsd: nsds) {
        if(nsd.getName().matches("itnamespace\\d+")) {
          LOG.info("Removing namespace="+nsd.getName());
          admin.deleteNamespace(nsd.getName());
        }
      }
    }

    enabledTables.clear();
    disabledTables.clear();
    deletedTables.clear();
    namespaceMap.clear();

    Connection connection = getConnection();
    connection.close();
    super.cleanUpCluster();
  }

  protected int getMinServerCount() {
    return SERVER_COUNT;
  }

  protected synchronized void setConnection(Connection connection){
    this.connection = connection;
  }

  protected synchronized Connection getConnection(){
    if (this.connection == null) {
      try {
        Connection connection = ConnectionFactory.createConnection(getConf());
        setConnection(connection);
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to establish connection.", e);
      }
    }
    return connection;
  }

  protected void verifyNamespaces() throws  IOException{
    Connection connection = getConnection();
    Admin admin = connection.getAdmin();
    // iterating concurrent map
    for (String nsName : namespaceMap.keySet()){
      try {
        Assert.assertTrue(
          "Namespace: " + nsName + " in namespaceMap does not exist",
          admin.getNamespaceDescriptor(nsName) != null);
      } catch (NamespaceNotFoundException nsnfe) {
        Assert.fail(
          "Namespace: " + nsName + " in namespaceMap does not exist: " + nsnfe.getMessage());
      }
    }
    admin.close();
  }

  protected void verifyTables() throws  IOException{
    Connection connection = getConnection();
    Admin admin = connection.getAdmin();
    // iterating concurrent map
    for (TableName tableName : enabledTables.keySet()){
      Assert.assertTrue("Table: " + tableName + " in enabledTables is not enabled",
          admin.isTableEnabled(tableName));
    }
    for (TableName tableName : disabledTables.keySet()){
      Assert.assertTrue("Table: " + tableName + " in disabledTables is not disabled",
          admin.isTableDisabled(tableName));
    }
    for (TableName tableName : deletedTables.keySet()){
      Assert.assertFalse("Table: " + tableName + " in deletedTables is not deleted",
          admin.tableExists(tableName));
    }
    admin.close();
  }

  @Test
  public void testAsUnitTest() throws Exception {
    runTest();
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    int ret = runTest();
    return ret;
  }

  private abstract class MasterAction{
    Connection connection = getConnection();

    abstract void perform() throws IOException;
  }

  private abstract class NamespaceAction extends MasterAction {
    final String nsTestConfigKey = "hbase.namespace.testKey";

    // NamespaceAction has implemented selectNamespace() shared by multiple namespace Actions
    protected NamespaceDescriptor selectNamespace(
        ConcurrentHashMap<String, NamespaceDescriptor> namespaceMap) {
      // synchronization to prevent removal from multiple threads
      synchronized (namespaceMap) {
        // randomly select namespace from namespaceMap
        if (namespaceMap.isEmpty()) {
          return null;
        }
        ArrayList<String> namespaceList = new ArrayList<>(namespaceMap.keySet());
        String randomKey = namespaceList.get(RandomUtils.nextInt(0, namespaceList.size()));
        NamespaceDescriptor randomNsd = namespaceMap.get(randomKey);
        // remove from namespaceMap
        namespaceMap.remove(randomKey);
        return randomNsd;
      }
    }
  }

  private class CreateNamespaceAction extends NamespaceAction {
    @Override
    void perform() throws IOException {
      Admin admin = connection.getAdmin();
      try {
        NamespaceDescriptor nsd;
        while (true) {
          nsd = createNamespaceDesc();
          try {
            if (admin.getNamespaceDescriptor(nsd.getName()) != null) {
              // the namespace has already existed.
              continue;
            } else {
              // currently, the code never return null - always throws exception if
              // namespace is not found - this just a defensive programming to make
              // sure null situation is handled in case the method changes in the
              // future.
              break;
            }
          } catch (NamespaceNotFoundException nsnfe) {
            // This is expected for a random generated NamespaceDescriptor
            break;
          }
        }
        LOG.info("Creating namespace:" + nsd);
        admin.createNamespace(nsd);
        NamespaceDescriptor freshNamespaceDesc = admin.getNamespaceDescriptor(nsd.getName());
        Assert.assertTrue("Namespace: " + nsd + " was not created", freshNamespaceDesc != null);
        namespaceMap.put(nsd.getName(), freshNamespaceDesc);
        LOG.info("Created namespace:" + freshNamespaceDesc);
      } catch (Exception e){
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }

    private NamespaceDescriptor createNamespaceDesc() {
      String namespaceName = "itnamespace" + String.format("%010d",
        RandomUtils.nextInt());
      NamespaceDescriptor nsd = NamespaceDescriptor.create(namespaceName).build();

      nsd.setConfiguration(
        nsTestConfigKey,
        String.format("%010d", RandomUtils.nextInt()));
      return nsd;
    }
  }

  private class ModifyNamespaceAction extends NamespaceAction {
    @Override
    void perform() throws IOException {
      NamespaceDescriptor selected = selectNamespace(namespaceMap);
      if (selected == null) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        String namespaceName = selected.getName();
        LOG.info("Modifying namespace :" + selected);
        NamespaceDescriptor modifiedNsd = NamespaceDescriptor.create(namespaceName).build();
        String nsValueNew;
        do {
          nsValueNew = String.format("%010d", RandomUtils.nextInt());
        } while (selected.getConfigurationValue(nsTestConfigKey).equals(nsValueNew));
        modifiedNsd.setConfiguration(nsTestConfigKey, nsValueNew);
        admin.modifyNamespace(modifiedNsd);
        NamespaceDescriptor freshNamespaceDesc = admin.getNamespaceDescriptor(namespaceName);
        Assert.assertTrue(
          "Namespace: " + selected + " was not modified",
          freshNamespaceDesc.getConfigurationValue(nsTestConfigKey).equals(nsValueNew));
        Assert.assertTrue(
          "Namespace: " + namespaceName + " does not exist",
          admin.getNamespaceDescriptor(namespaceName) != null);
        namespaceMap.put(namespaceName, freshNamespaceDesc);
        LOG.info("Modified namespace :" + freshNamespaceDesc);
      } catch (Exception e){
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }

  private class DeleteNamespaceAction extends NamespaceAction {
    @Override
    void perform() throws IOException {
      NamespaceDescriptor selected = selectNamespace(namespaceMap);
      if (selected == null) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        String namespaceName = selected.getName();
        LOG.info("Deleting namespace :" + selected);
        admin.deleteNamespace(namespaceName);
        try {
          if (admin.getNamespaceDescriptor(namespaceName) != null) {
            // the namespace still exists.
            Assert.assertTrue("Namespace: " + selected + " was not deleted", false);
          } else {
            LOG.info("Deleted namespace :" + selected);
          }
        } catch (NamespaceNotFoundException nsnfe) {
          // This is expected result
          LOG.info("Deleted namespace :" + selected);
        }
      } catch (Exception e){
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }

  private abstract class TableAction extends  MasterAction{
    // TableAction has implemented selectTable() shared by multiple table Actions
    protected TableDescriptor selectTable(ConcurrentHashMap<TableName, TableDescriptor> tableMap)
    {
      // synchronization to prevent removal from multiple threads
      synchronized (tableMap){
        // randomly select table from tableMap
        if (tableMap.isEmpty()) {
          return null;
        }
        ArrayList<TableName> tableList = new ArrayList<>(tableMap.keySet());
        TableName randomKey = tableList.get(RandomUtils.nextInt(0, tableList.size()));
        TableDescriptor randomTd = tableMap.remove(randomKey);
        return randomTd;
      }
    }
  }

  private class CreateTableAction extends TableAction {

    @Override
    void perform() throws IOException {
      Admin admin = connection.getAdmin();
      try {
        TableDescriptor td = createTableDesc();
        TableName tableName = td.getTableName();
        if ( admin.tableExists(tableName)){
          return;
        }
        String numRegionKey = String.format(NUM_REGIONS_KEY, this.getClass().getSimpleName());
        numRegions = getConf().getInt(numRegionKey, DEFAULT_NUM_REGIONS);
        byte[] startKey = Bytes.toBytes("row-0000000000");
        byte[] endKey = Bytes.toBytes("row-" + Integer.MAX_VALUE);
        LOG.info("Creating table:" + td);
        admin.createTable(td, startKey, endKey, numRegions);
        Assert.assertTrue("Table: " + td + " was not created", admin.tableExists(tableName));
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        Assert.assertTrue(
          "After create, Table: " + tableName + " in not enabled", admin.isTableEnabled(tableName));
        enabledTables.put(tableName, freshTableDesc);
        LOG.info("Created table:" + freshTableDesc);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }

    private TableDescriptor createTableDesc() {
      String tableName = String.format("ittable-%010d", RandomUtils.nextInt());
      String familyName = "cf-" + Math.abs(RandomUtils.nextInt());
      return TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName))
          .build();
    }
  }

  private class DisableTableAction extends TableAction {

    @Override
    void perform() throws IOException {

      TableDescriptor selected = selectTable(enabledTables);
      if (selected == null) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        TableName tableName = selected.getTableName();
        LOG.info("Disabling table :" + selected);
        admin.disableTable(tableName);
        Assert.assertTrue("Table: " + selected + " was not disabled",
            admin.isTableDisabled(tableName));
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        Assert.assertTrue(
          "After disable, Table: " + tableName + " is not disabled",
          admin.isTableDisabled(tableName));
        disabledTables.put(tableName, freshTableDesc);
        LOG.info("Disabled table :" + freshTableDesc);
      } catch (Exception e){
        LOG.warn("Caught exception in action: " + this.getClass());
        // TODO workaround
        // loose restriction for TableNotDisabledException/TableNotEnabledException thrown in sync
        // operations
        // 1) when enable/disable starts, the table state is changed to ENABLING/DISABLING (ZK node
        // in 1.x), which will be further changed to ENABLED/DISABLED once the operation completes
        // 2) if master failover happens in the middle of the enable/disable operation, the new
        // master will try to recover the tables in ENABLING/DISABLING state, as programmed in
        // AssignmentManager#recoverTableInEnablingState() and
        // AssignmentManager#recoverTableInDisablingState()
        // 3) after the new master initialization completes, the procedure tries to re-do the
        // enable/disable operation, which was already done. Ignore those exceptions before change
        // of behaviors of AssignmentManager in presence of PV2
        if (e instanceof TableNotEnabledException) {
          LOG.warn("Caught TableNotEnabledException in action: " + this.getClass());
          e.printStackTrace();
        } else {
          throw e;
        }
      } finally {
        admin.close();
      }
    }
  }

  private class EnableTableAction extends TableAction {

    @Override
    void perform() throws IOException {

      TableDescriptor selected = selectTable(disabledTables);
      if (selected == null ) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        TableName tableName = selected.getTableName();
        LOG.info("Enabling table :" + selected);
        admin.enableTable(tableName);
        Assert.assertTrue("Table: " + selected + " was not enabled",
            admin.isTableEnabled(tableName));
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        Assert.assertTrue(
          "After enable, Table: " + tableName + " in not enabled", admin.isTableEnabled(tableName));
        enabledTables.put(tableName, freshTableDesc);
        LOG.info("Enabled table :" + freshTableDesc);
      } catch (Exception e){
        LOG.warn("Caught exception in action: " + this.getClass());
        // TODO workaround
        // loose restriction for TableNotDisabledException/TableNotEnabledException thrown in sync
        // operations 1) when enable/disable starts, the table state is changed to
        // ENABLING/DISABLING (ZK node in 1.x), which will be further changed to ENABLED/DISABLED
        // once the operation completes 2) if master failover happens in the middle of the
        // enable/disable operation, the new master will try to recover the tables in
        // ENABLING/DISABLING state, as programmed in
        // AssignmentManager#recoverTableInEnablingState() and
        // AssignmentManager#recoverTableInDisablingState()
        // 3) after the new master initialization completes, the procedure tries to re-do the
        // enable/disable operation, which was already done. Ignore those exceptions before
        // change of behaviors of AssignmentManager in presence of PV2
        if (e instanceof TableNotDisabledException) {
          LOG.warn("Caught TableNotDisabledException in action: " + this.getClass());
          e.printStackTrace();
        } else {
          throw e;
        }
      } finally {
        admin.close();
      }
    }
  }

  private class DeleteTableAction extends TableAction {

    @Override
    void perform() throws IOException {

      TableDescriptor selected = selectTable(disabledTables);
      if (selected == null) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        TableName tableName = selected.getTableName();
        LOG.info("Deleting table :" + selected);
        admin.deleteTable(tableName);
        Assert.assertFalse("Table: " + selected + " was not deleted",
                admin.tableExists(tableName));
        deletedTables.put(tableName, selected);
        LOG.info("Deleted table :" + selected);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }


  private abstract class ColumnAction extends TableAction{
    // ColumnAction has implemented selectFamily() shared by multiple family Actions
    protected ColumnFamilyDescriptor selectFamily(TableDescriptor td) {
      if (td == null) {
        return null;
      }
      ColumnFamilyDescriptor[] families = td.getColumnFamilies();
      if (families.length == 0){
        LOG.info("No column families in table: " + td);
        return null;
      }
      ColumnFamilyDescriptor randomCfd = families[RandomUtils.nextInt(0, families.length)];
      return randomCfd;
    }
  }

  private class AddColumnFamilyAction extends ColumnAction {

    @Override
    void perform() throws IOException {
      TableDescriptor selected = selectTable(disabledTables);
      if (selected == null) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        ColumnFamilyDescriptor cfd = createFamilyDesc();
        if (selected.hasColumnFamily(cfd.getName())){
          LOG.info(new String(cfd.getName()) + " already exists in table "
              + selected.getTableName());
          return;
        }
        TableName tableName = selected.getTableName();
        LOG.info("Adding column family: " + cfd + " to table: " + tableName);
        admin.addColumnFamily(tableName, cfd);
        // assertion
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        Assert.assertTrue("Column family: " + cfd + " was not added",
            freshTableDesc.hasColumnFamily(cfd.getName()));
        Assert.assertTrue(
          "After add column family, Table: " + tableName + " is not disabled",
          admin.isTableDisabled(tableName));
        disabledTables.put(tableName, freshTableDesc);
        LOG.info("Added column family: " + cfd + " to table: " + tableName);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }

    private ColumnFamilyDescriptor createFamilyDesc() {
      String familyName = String.format("cf-%010d", RandomUtils.nextInt());
      return ColumnFamilyDescriptorBuilder.of(familyName);
    }
  }

  private class AlterFamilyVersionsAction extends ColumnAction {

    @Override
    void perform() throws IOException {
      TableDescriptor selected = selectTable(disabledTables);
      if (selected == null) {
        return;
      }
      ColumnFamilyDescriptor columnDesc = selectFamily(selected);
      if (columnDesc == null){
        return;
      }

      Admin admin = connection.getAdmin();
      int versions = RandomUtils.nextInt(0, 10) + 3;
      try {
        TableName tableName = selected.getTableName();
        LOG.info("Altering versions of column family: " + columnDesc + " to: " + versions +
            " in table: " + tableName);

        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(columnDesc)
            .setMinVersions(versions)
            .setMaxVersions(versions)
            .build();
        TableDescriptor td = TableDescriptorBuilder.newBuilder(selected)
            .modifyColumnFamily(cfd)
            .build();
        admin.modifyTable(td);

        // assertion
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        ColumnFamilyDescriptor freshColumnDesc = freshTableDesc.getColumnFamily(columnDesc.getName());
        Assert.assertEquals("Column family: " + columnDesc + " was not altered",
            freshColumnDesc.getMaxVersions(), versions);
        Assert.assertEquals("Column family: " + freshColumnDesc + " was not altered",
            freshColumnDesc.getMinVersions(), versions);
        Assert.assertTrue(
          "After alter versions of column family, Table: " + tableName + " is not disabled",
          admin.isTableDisabled(tableName));
        disabledTables.put(tableName, freshTableDesc);
        LOG.info("Altered versions of column family: " + columnDesc + " to: " + versions +
          " in table: " + tableName);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }

  private class AlterFamilyEncodingAction extends ColumnAction {

    @Override
    void perform() throws IOException {
      TableDescriptor selected = selectTable(disabledTables);
      if (selected == null) {
        return;
      }
      ColumnFamilyDescriptor columnDesc = selectFamily(selected);
      if (columnDesc == null){
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        TableName tableName = selected.getTableName();
        // possible DataBlockEncoding ids
        DataBlockEncoding[] possibleIds = {DataBlockEncoding.NONE, DataBlockEncoding.PREFIX,
                DataBlockEncoding.DIFF, DataBlockEncoding.FAST_DIFF, DataBlockEncoding.ROW_INDEX_V1};
        short id = possibleIds[RandomUtils.nextInt(0, possibleIds.length)].getId();
        LOG.info("Altering encoding of column family: " + columnDesc + " to: " + id +
            " in table: " + tableName);

        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(columnDesc)
            .setDataBlockEncoding(DataBlockEncoding.getEncodingById(id))
            .build();
        TableDescriptor td = TableDescriptorBuilder.newBuilder(selected)
            .modifyColumnFamily(cfd)
            .build();
        admin.modifyTable(td);

        // assertion
        TableDescriptor freshTableDesc = admin.getTableDescriptor(tableName);
        ColumnFamilyDescriptor freshColumnDesc = freshTableDesc.getColumnFamily(columnDesc.getName());
        Assert.assertEquals("Encoding of column family: " + columnDesc + " was not altered",
            freshColumnDesc.getDataBlockEncoding().getId(), id);
        Assert.assertTrue(
          "After alter encoding of column family, Table: " + tableName + " is not disabled",
          admin.isTableDisabled(tableName));
        disabledTables.put(tableName, freshTableDesc);
        LOG.info("Altered encoding of column family: " + freshColumnDesc + " to: " + id +
          " in table: " + tableName);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }

  private class DeleteColumnFamilyAction extends ColumnAction {

    @Override
    void perform() throws IOException {
      TableDescriptor selected = selectTable(disabledTables);
      ColumnFamilyDescriptor cfd = selectFamily(selected);
      if (selected == null || cfd == null) {
        return;
      }

      Admin admin = connection.getAdmin();
      try {
        if (selected.getColumnFamilyCount() < 2) {
          LOG.info("No enough column families to delete in table " + selected.getTableName());
          return;
        }
        TableName tableName = selected.getTableName();
        LOG.info("Deleting column family: " + cfd + " from table: " + tableName);
        admin.deleteColumnFamily(tableName, cfd.getName());
        // assertion
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        Assert.assertFalse("Column family: " + cfd + " was not added",
            freshTableDesc.hasColumnFamily(cfd.getName()));
        Assert.assertTrue(
          "After delete column family, Table: " + tableName + " is not disabled",
          admin.isTableDisabled(tableName));
        disabledTables.put(tableName, freshTableDesc);
        LOG.info("Deleted column family: " + cfd + " from table: " + tableName);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }

  private class AddRowAction extends ColumnAction {
    // populate tables
    @Override
    void perform() throws IOException {
      TableDescriptor selected = selectTable(enabledTables);
      if (selected == null ) {
        return;
      }

      Admin admin = connection.getAdmin();
      TableName tableName = selected.getTableName();
      try (Table table = connection.getTable(tableName)){
        ArrayList<HRegionInfo> regionInfos = new ArrayList<>(admin.getTableRegions(
            selected.getTableName()));
        int numRegions = regionInfos.size();
        // average number of rows to be added per action to each region
        int average_rows = 1;
        int numRows = average_rows * numRegions;
        LOG.info("Adding " + numRows + " rows to table: " + selected);
        for (int i = 0; i < numRows; i++){
          // nextInt(Integer.MAX_VALUE)) to return positive numbers only
          byte[] rowKey = Bytes.toBytes(
              "row-" + String.format("%010d", RandomUtils.nextInt()));
          ColumnFamilyDescriptor cfd = selectFamily(selected);
          if (cfd == null){
            return;
          }
          byte[] family = cfd.getName();
          byte[] qualifier = Bytes.toBytes("col-" + RandomUtils.nextInt() % 10);
          byte[] value = Bytes.toBytes("val-" + RandomStringUtils.randomAlphanumeric(10));
          Put put = new Put(rowKey);
          put.addColumn(family, qualifier, value);
          table.put(put);
        }
        TableDescriptor freshTableDesc = admin.getDescriptor(tableName);
        Assert.assertTrue(
          "After insert, Table: " + tableName + " in not enabled", admin.isTableEnabled(tableName));
        enabledTables.put(tableName, freshTableDesc);
        LOG.info("Added " + numRows + " rows to table: " + selected);
      } catch (Exception e) {
        LOG.warn("Caught exception in action: " + this.getClass());
        throw e;
      } finally {
        admin.close();
      }
    }
  }

  private enum ACTION {
    CREATE_NAMESPACE,
    MODIFY_NAMESPACE,
    DELETE_NAMESPACE,
    CREATE_TABLE,
    DISABLE_TABLE,
    ENABLE_TABLE,
    DELETE_TABLE,
    ADD_COLUMNFAMILY,
    DELETE_COLUMNFAMILY,
    ALTER_FAMILYVERSIONS,
    ALTER_FAMILYENCODING,
    ADD_ROW
  }

  private class Worker extends Thread {

    private Exception savedException;

    private ACTION action;

    @Override
    public void run() {
      while (running.get()) {
        // select random action
        ACTION selectedAction = ACTION.values()[RandomUtils.nextInt() % ACTION.values().length];
        this.action = selectedAction;
        LOG.info("Performing Action: " + selectedAction);

        try {
          switch (selectedAction) {
          case CREATE_NAMESPACE:
            new CreateNamespaceAction().perform();
            break;
          case MODIFY_NAMESPACE:
            new ModifyNamespaceAction().perform();
            break;
          case DELETE_NAMESPACE:
            new DeleteNamespaceAction().perform();
            break;
          case CREATE_TABLE:
            // stop creating new tables in the later stage of the test to avoid too many empty
            // tables
            if (create_table.get()) {
              new CreateTableAction().perform();
            }
            break;
          case ADD_ROW:
            new AddRowAction().perform();
            break;
          case DISABLE_TABLE:
            new DisableTableAction().perform();
            break;
          case ENABLE_TABLE:
            new EnableTableAction().perform();
            break;
          case DELETE_TABLE:
            // reduce probability of deleting table to 20%
            if (RandomUtils.nextInt(0, 100) < 20) {
              new DeleteTableAction().perform();
            }
            break;
          case ADD_COLUMNFAMILY:
            new AddColumnFamilyAction().perform();
            break;
          case DELETE_COLUMNFAMILY:
            // reduce probability of deleting column family to 20%
            if (RandomUtils.nextInt(0, 100) < 20) {
              new DeleteColumnFamilyAction().perform();
            }
            break;
          case ALTER_FAMILYVERSIONS:
            new AlterFamilyVersionsAction().perform();
            break;
          case ALTER_FAMILYENCODING:
            new AlterFamilyEncodingAction().perform();
            break;
          }
        } catch (Exception ex) {
          this.savedException = ex;
          return;
        }
      }
      LOG.info(this.getName() + " stopped");
    }

    public Exception getSavedException(){
      return this.savedException;
    }

    public ACTION getAction(){
      return this.action;
    }
  }

  private void checkException(List<Worker> workers){
    if(workers == null || workers.isEmpty())
      return;
    for (Worker worker : workers){
      Exception e = worker.getSavedException();
      if (e != null) {
        LOG.error("Found exception in thread: " + worker.getName());
        e.printStackTrace();
      }
      Assert.assertNull("Action failed: " + worker.getAction() + " in thread: "
          + worker.getName(), e);
    }
  }

  private int runTest() throws Exception {
    LOG.info("Starting the test");

    String runtimeKey = String.format(RUN_TIME_KEY, this.getClass().getSimpleName());
    long runtime = util.getConfiguration().getLong(runtimeKey, DEFAULT_RUN_TIME);

    String numThreadKey = String.format(NUM_THREADS_KEY, this.getClass().getSimpleName());
    numThreads = util.getConfiguration().getInt(numThreadKey, DEFAULT_NUM_THREADS);

    ArrayList<Worker> workers = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      checkException(workers);
      Worker worker = new Worker();
      LOG.info("Launching worker thread " + worker.getName());
      workers.add(worker);
      worker.start();
    }

    Threads.sleep(runtime / 2);
    LOG.info("Stopping creating new tables");
    create_table.set(false);
    Threads.sleep(runtime / 2);
    LOG.info("Runtime is up");
    running.set(false);

    checkException(workers);

    for (Worker worker : workers) {
      worker.join();
    }
    LOG.info("All Worker threads stopped");

    // verify
    LOG.info("Verify actions of all threads succeeded");
    checkException(workers);
    LOG.info("Verify namespaces");
    verifyNamespaces();
    LOG.info("Verify states of all tables");
    verifyTables();

    // RUN HBCK

    HBaseFsck hbck = null;
    try {
      LOG.info("Running hbck");
      hbck = HbckTestingUtil.doFsck(util.getConfiguration(), false);
      if (HbckTestingUtil.inconsistencyFound(hbck)) {
        // Find the inconsistency during HBCK. Leave table and namespace undropped so that
        // we can check outside the test.
        keepObjectsAtTheEnd = true;
      }
      HbckTestingUtil.assertNoErrors(hbck);
      LOG.info("Finished hbck");
    } finally {
      if (hbck != null) {
        hbck.close();
      }
    }
     return 0;
  }

  @Override
  public TableName getTablename() {
    return null; // This test is not inteded to run with stock Chaos Monkey
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return null; // This test is not inteded to run with stock Chaos Monkey
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    IntegrationTestDDLMasterFailover masterFailover = new IntegrationTestDDLMasterFailover();
    Connection connection = null;
    int ret = 1;
    try {
      // Initialize connection once, then pass to Actions
      LOG.debug("Setting up connection ...");
      connection = ConnectionFactory.createConnection(conf);
      masterFailover.setConnection(connection);
      ret = ToolRunner.run(conf, masterFailover, args);
    } catch (IOException e){
      LOG.error(HBaseMarkers.FATAL, "Failed to establish connection. Aborting test ...", e);
    } finally {
      connection = masterFailover.getConnection();
      if (connection != null){
        connection.close();
      }
      System.exit(ret);
    }
  }
}
