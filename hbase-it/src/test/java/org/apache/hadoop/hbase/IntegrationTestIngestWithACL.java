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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.test.LoadTestDataGeneratorWithACL;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * /**
 * An Integration class for tests that does something with the cluster while running
 * {@link LoadTestTool} to write and verify some data.
 * Verifies whether cells for users with only WRITE permissions are not read back
 * and cells with READ permissions are read back. 
 * Every operation happens in the user's specific context
 */
@Category(IntegrationTests.class)
public class IntegrationTestIngestWithACL extends IntegrationTestIngest {

  private static final char COLON = ':';
  public static final char HYPHEN = '-';
  private static final int SPECIAL_PERM_CELL_INSERTION_FACTOR = 100;
  public static final String OPT_SUPERUSER = "superuser";
  public static final String OPT_USERS = "userlist";
  public static final String OPT_AUTHN = "authinfo";
  private String superUser = "owner";
  private String userNames = "user1,user2,user3,user4"; 
  private String authnFileName;
  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(null);
    Configuration conf = util.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
    conf.set("hbase.coprocessor.master.classes", AccessController.class.getName());
    conf.set("hbase.coprocessor.region.classes", AccessController.class.getName());
    conf.setBoolean("hbase.security.access.early_out", false);
    // conf.set("hbase.superuser", "admin");
    super.setUpCluster();
  }

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    String[] args = super.getArgsForLoadTestTool(mode, modeSpecificArg, startKey, numKeys);
    List<String> tmp = new ArrayList<>(Arrays.asList(args));
    tmp.add(HYPHEN + LoadTestTool.OPT_GENERATOR);
    StringBuilder sb = new StringBuilder(LoadTestDataGeneratorWithACL.class.getName());
    sb.append(COLON);
    if (User.isHBaseSecurityEnabled(getConf())) {
      sb.append(authnFileName);
      sb.append(COLON);
    }
    sb.append(superUser);
    sb.append(COLON);
    sb.append(userNames);
    sb.append(COLON);
    sb.append(Integer.toString(SPECIAL_PERM_CELL_INSERTION_FACTOR));
    tmp.add(sb.toString());
    return tmp.toArray(new String[tmp.size()]);
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    super.addOptWithArg(OPT_SUPERUSER, "Super user name used to add the ACL permissions");
    super.addOptWithArg(OPT_USERS,
      "List of users to be added with the ACLs.  Should be comma seperated.");
    super.addOptWithArg(OPT_AUTHN,
      "The name of the properties file that contains"
        + " kerberos key tab file and principal definitions. The principal key in the file"
        + " should be of the form hbase.<username>.kerberos.principal. The keytab key in the"
        + " file should be of the form hbase.<username>.keytab.file. Example:"
        + "  hbase.user1.kerberos.principal=user1/fully.qualified.domain.name@YOUR-REALM.COM,"
        + " hbase.user1.keytab.file=<filelocation>.");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    if (cmd.hasOption(OPT_SUPERUSER)) {
      superUser = cmd.getOptionValue(OPT_SUPERUSER);
    }
    if (cmd.hasOption(OPT_USERS)) {
      userNames = cmd.getOptionValue(OPT_USERS);
    }
    if (User.isHBaseSecurityEnabled(getConf())) {
      boolean authFileNotFound = false;
      if (cmd.hasOption(OPT_AUTHN)) {
        authnFileName = cmd.getOptionValue(OPT_AUTHN);
        if (StringUtils.isEmpty(authnFileName)) {
          authFileNotFound = true;
        }
      } else {
        authFileNotFound = true;
      }
      if (authFileNotFound) {
        super.printUsage();
        System.exit(EXIT_FAILURE);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngestWithACL(), args);
    System.exit(ret);
  }
}
