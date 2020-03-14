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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mapreduce.ExportUtils;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.HadoopSecurityEnabledUserProviderForTesting;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.access.SecureTestUtil.AccessTestAction;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class})
public class TestSecureExport {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSecureExport.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSecureExport.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static MiniKdc KDC;
  private static final File KEYTAB_FILE = new File(UTIL.getDataTestDir("keytab").toUri().getPath());
  private static String USERNAME;
  private static String SERVER_PRINCIPAL;
  private static String HTTP_PRINCIPAL;
  private static final String FAMILYA_STRING = "fma";
  private static final String FAMILYB_STRING = "fma";
  private static final byte[] FAMILYA = Bytes.toBytes(FAMILYA_STRING);
  private static final byte[] FAMILYB = Bytes.toBytes(FAMILYB_STRING);
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final byte[] QUAL = Bytes.toBytes("qual");
  private static final String LOCALHOST = "localhost";
  private static final long NOW = System.currentTimeMillis();
  // user granted with all global permission
  private static final String USER_ADMIN = "admin";
  // user is table owner. will have all permissions on table
  private static final String USER_OWNER = "owner";
  // user with rx permissions.
  private static final String USER_RX = "rxuser";
  // user with exe-only permissions.
  private static final String USER_XO = "xouser";
  // user with read-only permissions.
  private static final String USER_RO = "rouser";
  // user with no permissions
  private static final String USER_NONE = "noneuser";
  private static final String PRIVATE = "private";
  private static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  private static final String TOPSECRET = "topsecret";
  @Rule
  public final TestName name = new TestName();
  private static void setUpKdcServer() throws Exception {
    KDC = UTIL.setupMiniKdc(KEYTAB_FILE);
    USERNAME = UserGroupInformation.getLoginUser().getShortUserName();
    SERVER_PRINCIPAL = USERNAME + "/" + LOCALHOST;
    HTTP_PRINCIPAL = "HTTP/" + LOCALHOST;
    KDC.createPrincipal(KEYTAB_FILE,
      SERVER_PRINCIPAL,
      HTTP_PRINCIPAL,
      USER_ADMIN + "/" + LOCALHOST,
      USER_OWNER + "/" + LOCALHOST,
      USER_RX + "/" + LOCALHOST,
      USER_RO + "/" + LOCALHOST,
      USER_XO + "/" + LOCALHOST,
      USER_NONE + "/" + LOCALHOST);
  }

  private static User getUserByLogin(final String user) throws IOException {
    return User.create(UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        getPrinciple(user), KEYTAB_FILE.getAbsolutePath()));
  }

  private static String getPrinciple(final String user) {
    return user + "/" + LOCALHOST + "@" + KDC.getRealm();
  }

  private static void setUpClusterKdc() throws Exception {
    HBaseKerberosUtils.setSecuredConfiguration(UTIL.getConfiguration(),
        SERVER_PRINCIPAL + "@" + KDC.getRealm(), HTTP_PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSSLConfiguration(UTIL, TestSecureExport.class);

    UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        UTIL.getConfiguration().get(
            CoprocessorHost.REGION_COPROCESSOR_CONF_KEY) + "," + Export.class.getName());
  }

  private static void addLabels(final Configuration conf, final List<String> users,
      final List<String> labels) throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsProtos.VisibilityLabelsResponse> action
      = () -> {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.addLabels(conn, labels.toArray(new String[labels.size()]));
          for (String user : users) {
            VisibilityClient.setAuths(conn, labels.toArray(new String[labels.size()]), user);
          }
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      };
    getUserByLogin(USER_ADMIN).runAs(action);
  }

  @Before
  public void announce() {
    LOG.info("Running " + name.getMethodName());
  }

  @After
  public void cleanup() throws IOException {
  }

  private static void clearOutput(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(UTIL.getConfiguration());
    if (fs.exists(path)) {
      assertEquals(true, fs.delete(path, true));
    }
  }

  /**
   * Sets the security firstly for getting the correct default realm.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    UserProvider.setUserProviderForTesting(UTIL.getConfiguration(),
        HadoopSecurityEnabledUserProviderForTesting.class);
    setUpKdcServer();
    SecureTestUtil.enableSecurity(UTIL.getConfiguration());
    UTIL.getConfiguration().setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);
    VisibilityTestUtil.enableVisiblityLabels(UTIL.getConfiguration());
    SecureTestUtil.verifyConfiguration(UTIL.getConfiguration());
    setUpClusterKdc();
    UTIL.startMiniCluster();
    UTIL.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);
    UTIL.waitUntilAllRegionsAssigned(VisibilityConstants.LABELS_TABLE_NAME);
    UTIL.waitTableEnabled(PermissionStorage.ACL_TABLE_NAME, 50000);
    UTIL.waitTableEnabled(VisibilityConstants.LABELS_TABLE_NAME, 50000);
    SecureTestUtil.grantGlobal(UTIL, USER_ADMIN,
            Permission.Action.ADMIN,
            Permission.Action.CREATE,
            Permission.Action.EXEC,
            Permission.Action.READ,
            Permission.Action.WRITE);
    addLabels(UTIL.getConfiguration(), Arrays.asList(USER_OWNER),
            Arrays.asList(PRIVATE, CONFIDENTIAL, SECRET, TOPSECRET));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (KDC != null) {
      KDC.stop();
    }
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test the ExportEndpoint's access levels. The {@link Export} test is ignored
   * since the access exceptions cannot be collected from the mappers.
   */
  @Test
  public void testAccessCase() throws Throwable {
    final String exportTable = name.getMethodName();
    TableDescriptor exportHtd = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILYA))
            .setOwnerString(USER_OWNER)
            .build();
    SecureTestUtil.createTable(UTIL, exportHtd, new byte[][]{Bytes.toBytes("s")});
    SecureTestUtil.grantOnTable(UTIL, USER_RO,
            TableName.valueOf(exportTable), null, null,
            Permission.Action.READ);
    SecureTestUtil.grantOnTable(UTIL, USER_RX,
            TableName.valueOf(exportTable), null, null,
            Permission.Action.READ,
            Permission.Action.EXEC);
    SecureTestUtil.grantOnTable(UTIL, USER_XO,
            TableName.valueOf(exportTable), null, null,
            Permission.Action.EXEC);
    assertEquals(4, PermissionStorage
        .getTablePermissions(UTIL.getConfiguration(), TableName.valueOf(exportTable)).size());
    AccessTestAction putAction = () -> {
      Put p = new Put(ROW1);
      p.addColumn(FAMILYA, Bytes.toBytes("qual_0"), NOW, QUAL);
      p.addColumn(FAMILYA, Bytes.toBytes("qual_1"), NOW, QUAL);
      try (Connection conn = ConnectionFactory.createConnection(UTIL.getConfiguration());
              Table t = conn.getTable(TableName.valueOf(exportTable))) {
        t.put(p);
      }
      return null;
    };
    // no hdfs access.
    SecureTestUtil.verifyAllowed(putAction,
      getUserByLogin(USER_ADMIN),
      getUserByLogin(USER_OWNER));
    SecureTestUtil.verifyDenied(putAction,
      getUserByLogin(USER_RO),
      getUserByLogin(USER_XO),
      getUserByLogin(USER_RX),
      getUserByLogin(USER_NONE));

    final FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    final Path openDir = fs.makeQualified(new Path("testAccessCase"));
    fs.mkdirs(openDir);
    fs.setPermission(openDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    final Path output = fs.makeQualified(new Path(openDir, "output"));
    AccessTestAction exportAction = () -> {
      try {
        String[] args = new String[]{exportTable, output.toString()};
        Map<byte[], Export.Response> result
                = Export.run(new Configuration(UTIL.getConfiguration()), args);
        long rowCount = 0;
        long cellCount = 0;
        for (Export.Response r : result.values()) {
          rowCount += r.getRowCount();
          cellCount += r.getCellCount();
        }
        assertEquals(1, rowCount);
        assertEquals(2, cellCount);
        return null;
      } catch (ServiceException | IOException ex) {
        throw ex;
      } catch (Throwable ex) {
        LOG.error(ex.toString(), ex);
        throw new Exception(ex);
      } finally {
        if (fs.exists(new Path(openDir, "output"))) {
          // if export completes successfully, every file under the output directory should be
          // owned by the current user, not the hbase service user.
          FileStatus outputDirFileStatus = fs.getFileStatus(new Path(openDir, "output"));
          String currentUserName = User.getCurrent().getShortName();
          assertEquals("Unexpected file owner", currentUserName, outputDirFileStatus.getOwner());

          FileStatus[] outputFileStatus = fs.listStatus(new Path(openDir, "output"));
          for (FileStatus fileStatus: outputFileStatus) {
            assertEquals("Unexpected file owner", currentUserName, fileStatus.getOwner());
          }
        } else {
          LOG.info("output directory doesn't exist. Skip check");
        }

        clearOutput(output);
      }
    };
    SecureTestUtil.verifyDenied(exportAction,
      getUserByLogin(USER_RO),
      getUserByLogin(USER_XO),
      getUserByLogin(USER_NONE));
    SecureTestUtil.verifyAllowed(exportAction,
      getUserByLogin(USER_ADMIN),
      getUserByLogin(USER_OWNER),
      getUserByLogin(USER_RX));
    AccessTestAction deleteAction = () -> {
      UTIL.deleteTable(TableName.valueOf(exportTable));
      return null;
    };
    SecureTestUtil.verifyAllowed(deleteAction, getUserByLogin(USER_OWNER));
    fs.delete(openDir, true);
  }

  @Test
  @org.junit.Ignore // See HBASE-23990
  public void testVisibilityLabels() throws IOException, Throwable {
    final String exportTable = name.getMethodName() + "_export";
    final String importTable = name.getMethodName() + "_import";
    final TableDescriptor exportHtd = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(exportTable))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILYA))
            .setOwnerString(USER_OWNER)
            .build();
    SecureTestUtil.createTable(UTIL, exportHtd, new byte[][]{Bytes.toBytes("s")});
    AccessTestAction putAction = () -> {
      Put p1 = new Put(ROW1);
      p1.addColumn(FAMILYA, QUAL, NOW, QUAL);
      p1.setCellVisibility(new CellVisibility(SECRET));
      Put p2 = new Put(ROW2);
      p2.addColumn(FAMILYA, QUAL, NOW, QUAL);
      p2.setCellVisibility(new CellVisibility(PRIVATE + " & " + CONFIDENTIAL));
      Put p3 = new Put(ROW3);
      p3.addColumn(FAMILYA, QUAL, NOW, QUAL);
      p3.setCellVisibility(new CellVisibility("!" + CONFIDENTIAL + " & " + TOPSECRET));
      try (Connection conn = ConnectionFactory.createConnection(UTIL.getConfiguration());
              Table t = conn.getTable(TableName.valueOf(exportTable))) {
        t.put(p1);
        t.put(p2);
        t.put(p3);
      }
      return null;
    };
    SecureTestUtil.verifyAllowed(putAction, getUserByLogin(USER_OWNER));
    List<Pair<List<String>, Integer>> labelsAndRowCounts = new LinkedList<>();
    labelsAndRowCounts.add(new Pair<>(Arrays.asList(SECRET), 1));
    labelsAndRowCounts.add(new Pair<>(Arrays.asList(PRIVATE, CONFIDENTIAL), 1));
    labelsAndRowCounts.add(new Pair<>(Arrays.asList(TOPSECRET), 1));
    labelsAndRowCounts.add(new Pair<>(Arrays.asList(TOPSECRET, CONFIDENTIAL), 0));
    labelsAndRowCounts.add(new Pair<>(Arrays.asList(TOPSECRET, CONFIDENTIAL, PRIVATE, SECRET), 2));
    for (final Pair<List<String>, Integer> labelsAndRowCount : labelsAndRowCounts) {
      final List<String> labels = labelsAndRowCount.getFirst();
      final int rowCount = labelsAndRowCount.getSecond();
      //create a open permission directory.
      final Path openDir = new Path("testAccessCase");
      final FileSystem fs = openDir.getFileSystem(UTIL.getConfiguration());
      fs.mkdirs(openDir);
      fs.setPermission(openDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      final Path output = fs.makeQualified(new Path(openDir, "output"));
      AccessTestAction exportAction = () -> {
        StringBuilder buf = new StringBuilder();
        labels.forEach(v -> buf.append(v).append(","));
        buf.deleteCharAt(buf.length() - 1);
        try {
          String[] args = new String[]{
            "-D " + ExportUtils.EXPORT_VISIBILITY_LABELS + "=" + buf.toString(),
            exportTable,
            output.toString(),};
          Export.run(new Configuration(UTIL.getConfiguration()), args);
          return null;
        } catch (ServiceException | IOException ex) {
          throw ex;
        } catch (Throwable ex) {
          throw new Exception(ex);
        }
      };
      SecureTestUtil.verifyAllowed(exportAction, getUserByLogin(USER_OWNER));
      final TableDescriptor importHtd = TableDescriptorBuilder
              .newBuilder(TableName.valueOf(importTable))
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILYB))
              .setOwnerString(USER_OWNER)
              .build();
      SecureTestUtil.createTable(UTIL, importHtd, new byte[][]{Bytes.toBytes("s")});
      AccessTestAction importAction = () -> {
        String[] args = new String[]{
          "-D" + Import.CF_RENAME_PROP + "=" + FAMILYA_STRING + ":" + FAMILYB_STRING,
          importTable,
          output.toString()
        };
        assertEquals(0, ToolRunner.run(
            new Configuration(UTIL.getConfiguration()), new Import(), args));
        return null;
      };
      SecureTestUtil.verifyAllowed(importAction, getUserByLogin(USER_OWNER));
      AccessTestAction scanAction = () -> {
        Scan scan = new Scan();
        scan.setAuthorizations(new Authorizations(labels));
        try (Connection conn = ConnectionFactory.createConnection(UTIL.getConfiguration());
                Table table = conn.getTable(importHtd.getTableName());
                ResultScanner scanner = table.getScanner(scan)) {
          int count = 0;
          for (Result r : scanner) {
            ++count;
          }
          assertEquals(rowCount, count);
        }
        return null;
      };
      SecureTestUtil.verifyAllowed(scanAction, getUserByLogin(USER_OWNER));
      AccessTestAction deleteAction = () -> {
        UTIL.deleteTable(importHtd.getTableName());
        return null;
      };
      SecureTestUtil.verifyAllowed(deleteAction, getUserByLogin(USER_OWNER));
      clearOutput(output);
    }
    AccessTestAction deleteAction = () -> {
      UTIL.deleteTable(exportHtd.getTableName());
      return null;
    };
    SecureTestUtil.verifyAllowed(deleteAction, getUserByLogin(USER_OWNER));
  }
}
