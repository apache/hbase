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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.test.LoadTestDataGeneratorWithACL;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;
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
  private String superUser = "owner";
  private String userNames = "user1,user2,user3,user4"; 
  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(null);
    Configuration conf = util.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
    conf.set("hbase.coprocessor.master.classes", AccessController.class.getName());
    conf.set("hbase.coprocessor.region.classes", AccessController.class.getName());
    // conf.set("hbase.superuser", "admin");
    super.setUpCluster();
  }

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    String[] args = super.getArgsForLoadTestTool(mode, modeSpecificArg, startKey, numKeys);
    List<String> tmp = new ArrayList<String>(Arrays.asList(args));
    tmp.add(HYPHEN + LoadTestTool.OPT_GENERATOR);
    StringBuilder sb = new StringBuilder(LoadTestDataGeneratorWithACL.class.getName());
    sb.append(COLON);
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
    super.addOptWithArg(OPT_SUPERUSER,
        "Super user name used to add the ACL permissions");
    super.addOptWithArg(OPT_USERS,
        "List of users to be added with the ACLs.  Should be comma seperated.");
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
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngestWithACL(), args);
    System.exit(ret);
  }
}
