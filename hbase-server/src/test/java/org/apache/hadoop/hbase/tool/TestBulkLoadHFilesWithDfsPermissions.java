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
package org.apache.hadoop.hbase.tool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end bulk load test on a MiniDFSCluster with HDFS permissions enabled and three distinct
 * identities: the HBase service user (which runs the mini cluster and must not be the HDFS
 * superuser), the bulk load client (which owns the input HFiles and the target table) and an
 * unrelated attacker user.
 * <p>
 * A single HFile spanning the table split point is loaded, so BulkLoadHFilesTool has to split it
 * into {@code .tmp/<uuid>.bottom} and {@code .tmp/<uuid>.top} inside the client's own directory.
 * The test verifies that the load succeeds without any world-writable permission on the split
 * artifacts (they are handed to the service user through the staging directory of
 * {@code SecureBulkLoadManager}, which is protected by a random directory name), and that a third
 * user cannot tamper with the split directory.
 */
@Tag(MiscTests.TAG)
@Tag(LargeTests.TAG)
public class TestBulkLoadHFilesWithDfsPermissions {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE = TableName.valueOf("bulkload_multiuser");
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] SPLIT_KEY = Bytes.toBytes("m");
  private static final String[] ROWS = { "a1", "b1", "y1", "z1" };

  // The MiniDFSCluster superuser is the OS user running the test (the one that started the
  // NameNode). The identities below are simple-auth identities enforced by HDFS permissions.
  private static UserGroupInformation originalLogin;
  private static UserGroupInformation serviceUgi;
  private static UserGroupInformation clientUgi;
  private static UserGroupInformation attackerUgi;
  private static FileSystem superuserFs;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean("dfs.permissions", true);
    UTIL.getConfiguration().setBoolean("dfs.permissions.enabled", true);
    // Start the DFS cluster first, under the test-runner identity: the test runner becomes the
    // HDFS superuser of the mini cluster.
    UTIL.startMiniDFSCluster(1);
    UTIL.startMiniZKCluster();
    superuserFs = UTIL.getDFSCluster().getFileSystem();

    originalLogin = UserGroupInformation.getLoginUser();
    serviceUgi = UserGroupInformation.createUserForTesting("hbase_service_user",
      new String[] { "hbase_service_group" });
    clientUgi = UserGroupInformation.createUserForTesting("bulkload_client_user",
      new String[] { "bulkload_client_group" });
    attackerUgi = UserGroupInformation.createUserForTesting("bulkload_attacker_user",
      new String[] { "bulkload_attacker_group" });

    // Hand the cluster data directories to the service user before it starts using them.
    Path dataDir = UTIL.getDataTestDirOnTestFS();
    superuserFs.mkdirs(dataDir);
    superuserFs.setOwner(dataDir, serviceUgi.getUserName(), serviceUgi.getUserName());
    superuserFs.setOwner(dataDir.getParent(), serviceUgi.getUserName(), serviceUgi.getUserName());

    // Start HBase under the service identity: every FileSystem the servers create is bound to
    // "hbase_service_user", which is not the HDFS superuser.
    UserGroupInformation.setLoginUser(serviceUgi);
    UTIL.startMiniHBaseCluster();

    // Two regions: ("", "m") and ["m", ""). A single HFile with rows on both sides of "m" has to
    // be split by BulkLoadHFilesTool before loading.
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    UTIL.getAdmin().createTable(desc, new byte[][] { SPLIT_KEY });
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UserGroupInformation.setLoginUser(originalLogin);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBulkLoadWithSplitUnderHdfsPermissions() throws Exception {
    // Precondition: the HBase service user is not the HDFS superuser (it cannot write outside
    // of the directories it owns).
    assertThrows(AccessControlException.class, () -> runAs(serviceUgi, () -> FileSystem
      .get(UTIL.getConfiguration()).create(new Path("/superuser-check-" + UUID.randomUUID()))));

    // The client prepares the input HFile in its own directory, spanning the split point.
    Path inputDir = new Path("/bulkload-input-" + UUID.randomUUID());
    superuserFs.mkdirs(inputDir);
    superuserFs.setOwner(inputDir, clientUgi.getUserName(), clientUgi.getUserName());
    runAs(clientUgi, () -> {
      FileSystem clientFs = FileSystem.get(UTIL.getConfiguration());
      Path familyDir = new Path(inputDir, Bytes.toString(FAMILY));
      clientFs.mkdirs(familyDir);
      writeHFile(clientFs, new Path(familyDir, "spanning.hfile"));
      return null;
    });

    // Bulk load as the client. The HFile does not fit into a single region, so it is split into
    // .tmp/<uuid>.bottom and .tmp/<uuid>.top in the client's directory first, then the split
    // outputs are moved through the staging directory of SecureBulkLoadManager into the regions.
    runAs(clientUgi, () -> {
      BulkLoadHFiles.create(UTIL.getConfiguration()).bulkLoad(TABLE, inputDir);
      return null;
    });

    FileSystem clientFs = runAs(clientUgi, () -> FileSystem.get(UTIL.getConfiguration()));
    Path tmpDir = new Path(new Path(inputDir, Bytes.toString(FAMILY)), BulkLoadHFilesTool.TMP_DIR);
    assertTrue(clientFs.exists(tmpDir), "split did not happen, " + tmpDir + " is missing");
    assertEquals(0, clientFs.listStatus(tmpDir).length, "split outputs were not consumed");

    // The split directory must not carry loose permissions: neither the group nor others may
    // write to it. (BulkLoadHFilesTool used to chmod it to 777.)
    FsPermission tmpPerm = clientFs.getFileStatus(tmpDir).getPermission();
    assertFalse(tmpPerm.getOtherAction().implies(FsAction.WRITE),
      "split tmp dir must not be world-writable: " + tmpPerm);
    assertFalse(tmpPerm.getGroupAction().implies(FsAction.WRITE),
      "split tmp dir must not be group-writable: " + tmpPerm);

    // An unrelated user must not be able to tamper with the split directory, not even after the
    // load has completed.
    assertThrows(AccessControlException.class, () -> runAs(attackerUgi, () -> {
      FileSystem attackerFs = FileSystem.get(UTIL.getConfiguration());
      attackerFs.create(new Path(tmpDir, "evil"));
      return null;
    }));

    // All rows must have been loaded into the correct regions with their values preserved.
    Set<String> rows = new TreeSet<>();
    try (Table table = UTIL.getConnection().getTable(TABLE);
      ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result : scanner) {
        String row = Bytes.toString(result.getRow());
        rows.add(row);
        assertEquals(row, Bytes.toString(result.getValue(FAMILY, QUALIFIER)));
      }
    }
    assertEquals(new TreeSet<>(Arrays.asList(ROWS)), rows);
  }

  private static <T> T runAs(UserGroupInformation ugi, PrivilegedExceptionAction<T> action)
    throws Exception {
    return ugi.doAs(action);
  }

  private static void writeHFile(FileSystem fs, Path path) throws IOException {
    HFile.Writer writer = HFile.getWriterFactory(UTIL.getConfiguration(), CacheConfig.DISABLED)
      .withPath(fs, path).withFileContext(new HFileContextBuilder().build()).create();
    try {
      long ts = EnvironmentEdgeManager.currentTime();
      for (String row : ROWS) {
        writer.append(new KeyValue(Bytes.toBytes(row), FAMILY, QUALIFIER, ts, Bytes.toBytes(row)));
      }
    } finally {
      writer.close();
    }
  }
}
