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

package org.apache.hadoop.hbase.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * An integration test which checks that the znodes in zookeeper and data in the FileSystem
 * are protected for secure HBase deployments.
 * This test is intended to be run on clusters with kerberos authorization for HBase and ZooKeeper.
 *
 * If hbase.security.authentication is not set to kerberos, the test does not run unless -f is
 * specified which bypasses the check. It is recommended to always run with -f on secure clusters
 * so that the test checks the actual end result, not the configuration.
 *
 * The test should be run as hbase user with kinit / TGT cached since it accesses HDFS.
 * <p>
 * Example usage:
 *   hbase org.apache.hadoop.hbase.test.IntegrationTestZnodeACLs -h
 */
@Category(IntegrationTests.class)
public class IntegrationTestZKAndFSPermissions extends AbstractHBaseTool {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestZKAndFSPermissions.class);

  private String superUser;
  private String masterPrincipal;
  private boolean isForce;
  private String fsPerms;
  private boolean skipFSCheck;
  private boolean skipZKCheck;

  public static final String FORCE_CHECK_ARG = "f";
  public static final String PRINCIPAL_ARG = "p";
  public static final String SUPERUSER_ARG = "s";
  public static final String FS_PERMS = "fs_perms";
  public static final String SKIP_CHECK_FS = "skip_fs_check";
  public static final String SKIP_CHECK_ZK = "skip_zk_check";

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  @Override
  protected void addOptions() {
    addOptNoArg(FORCE_CHECK_ARG, "Whether to skip configuration lookup and assume a secure setup");
    addOptWithArg(PRINCIPAL_ARG, "The principal for zk authorization");
    addOptWithArg(SUPERUSER_ARG, "The principal for super user");
    addOptWithArg(FS_PERMS,      "FS permissions, ex. 700, 750, etc. Defaults to 700");
    addOptNoArg(SKIP_CHECK_FS, "Whether to skip checking FS permissions");
    addOptNoArg(SKIP_CHECK_ZK,   "Whether to skip checking ZK permissions");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    isForce = cmd.hasOption(FORCE_CHECK_ARG);
    masterPrincipal = getShortUserName(conf.get(SecurityConstants.MASTER_KRB_PRINCIPAL));
    superUser = cmd.getOptionValue(SUPERUSER_ARG, conf.get("hbase.superuser"));
    masterPrincipal = cmd.getOptionValue(PRINCIPAL_ARG, masterPrincipal);
    fsPerms = cmd.getOptionValue(FS_PERMS, "700");
    skipFSCheck = cmd.hasOption(SKIP_CHECK_FS);
    skipZKCheck = cmd.hasOption(SKIP_CHECK_ZK);
  }

  private String getShortUserName(String principal) {
    for (int i = 0; i < principal.length(); i++) {
      if (principal.charAt(i) == '/' || principal.charAt(i) == '@') {
        return principal.substring(0, i);
      }
    }
    return principal;
  }

  @Override
  protected int doWork() throws Exception {
    if (!isForce) {
      if (!"kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication"))) {
        LOG.warn("hbase.security.authentication is not kerberos, and -f is not supplied. Skip "
            + "running the test");
        return 0;
      }
    }

    if (!skipZKCheck) {
      testZNodeACLs();
    } if (!skipFSCheck) {
      testFSPerms();
    }
    return 0;
  }

  private void testZNodeACLs() throws IOException, KeeperException, InterruptedException {

    ZKWatcher watcher = new ZKWatcher(conf, "IntegrationTestZnodeACLs", null);
    RecoverableZooKeeper zk = ZKUtil.connect(this.conf, watcher);

    String baseZNode = watcher.getZNodePaths().baseZNode;

    LOG.info("");
    LOG.info("***********************************************************************************");
    LOG.info("Checking ZK permissions, root znode: " + baseZNode);
    LOG.info("***********************************************************************************");
    LOG.info("");

    checkZnodePermsRecursive(watcher, zk, baseZNode);

    LOG.info("Checking ZK permissions: SUCCESS");
  }

  private void checkZnodePermsRecursive(ZKWatcher watcher,
      RecoverableZooKeeper zk, String znode) throws KeeperException, InterruptedException {

    boolean expectedWorldReadable = watcher.getZNodePaths().isClientReadable(znode);

    assertZnodePerms(zk, znode, expectedWorldReadable);

    try {
      List<String> children = zk.getChildren(znode, false);

      for (String child : children) {
        checkZnodePermsRecursive(watcher, zk, ZNodePaths.joinZNode(znode, child));
      }
    } catch (KeeperException ke) {
      // if we are not authenticated for listChildren, it is fine.
      if (ke.code() != Code.NOAUTH && ke.code() != Code.NONODE) {
        throw ke;
      }
    }
  }

  private void assertZnodePerms(RecoverableZooKeeper zk, String znode,
      boolean expectedWorldReadable) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    List<ACL> acls;
    try {
      acls = zk.getZooKeeper().getACL(znode, stat);
    } catch (NoNodeException ex) {
      LOG.debug("Caught exception for missing znode", ex);
      // the znode is deleted. Probably it was a temporary znode (like RIT).
      return;
    }
    String[] superUsers = superUser == null ? null : superUser.split(",");

    LOG.info("Checking ACLs for znode znode:" + znode + " acls:" + acls);

    for (ACL acl : acls) {
      int perms = acl.getPerms();
      Id id = acl.getId();
      // We should only set at most 3 possible ACL for 3 Ids. One for everyone, one for superuser
      // and one for the hbase user
      if (Ids.ANYONE_ID_UNSAFE.equals(id)) {
        // everyone should be set only if we are expecting this znode to be world readable
        assertTrue(expectedWorldReadable);
        // assert that anyone can only read
        assertEquals(perms, Perms.READ);
      } else if (superUsers != null && ZKWatcher.isSuperUserId(superUsers, id)) {
        // assert that super user has all the permissions
        assertEquals(perms, Perms.ALL);
      } else if (new Id("sasl", masterPrincipal).equals(id)) {
        // hbase.master.kerberos.principal?
        assertEquals(perms, Perms.ALL);
      } else {
        fail("An ACL is found which is not expected for the znode:" + znode + " , ACL:" + acl);
      }
    }
  }

  private void testFSPerms() throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);

    LOG.info("");
    LOG.info("***********************************************************************************");
    LOG.info("Checking FS permissions for root dir:" + rootDir);
    LOG.info("***********************************************************************************");
    LOG.info("");
    FileSystem fs = rootDir.getFileSystem(conf);

    short expectedPerms = Short.valueOf(fsPerms, 8);

    assertEquals(
      FsPermission.createImmutable(expectedPerms),
      fs.getFileStatus(rootDir).getPermission());

    LOG.info("Checking FS permissions: SUCCESS");
  }

  public static void main(String[] args) throws Exception {
    Configuration configuration = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(configuration);
    IntegrationTestZKAndFSPermissions tool = new IntegrationTestZKAndFSPermissions();
    int ret = ToolRunner.run(configuration, tool, args);
    System.exit(ret);
  }
}
