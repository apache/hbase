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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.LoadTestDataGeneratorWithVisibilityLabels;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)
public class IntegrationTestIngestWithVisibilityLabels extends IntegrationTestIngest {

  private static final char COMMA = ',';
  private static final char COLON = ':';
  private static final String[] LABELS = { "secret", "topsecret", "confidential", "public",
      "private" };
  private static final String[] VISIBILITY_EXPS = { "secret & confidential & !private",
      "topsecret | confidential", "confidential & private", "public", "topsecret & private",
      "!public | private", "(secret | topsecret) & private" };
  private static final List<List<String>> AUTHS = new ArrayList<List<String>>();

  static {
    ArrayList<String> tmp = new ArrayList<String>();
    tmp.add("secret");
    tmp.add("confidential");
    AUTHS.add(tmp);
    tmp = new ArrayList<String>();
    tmp.add("topsecret");
    AUTHS.add(tmp);
    tmp = new ArrayList<String>();
    tmp.add("confidential");
    tmp.add("private");
    AUTHS.add(tmp);
    tmp = new ArrayList<String>();
    tmp.add("public");
    AUTHS.add(tmp);
    tmp = new ArrayList<String>();
    tmp.add("topsecret");
    tmp.add("private");
    AUTHS.add(tmp);
    tmp = new ArrayList<String>();
    tmp.add("confidential");
    AUTHS.add(tmp);
    tmp = new ArrayList<String>();
    tmp.add("topsecret");
    tmp.add("private");
    AUTHS.add(tmp);
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(null);
    Configuration conf = util.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
    conf.set("hbase.coprocessor.master.classes", VisibilityController.class.getName());
    conf.set("hbase.coprocessor.region.classes", VisibilityController.class.getName());
    conf.set("hbase.superuser", "admin," + User.getCurrent().getName());
    super.setUpCluster();
    addLabels();
  }

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    String[] args = super.getArgsForLoadTestTool(mode, modeSpecificArg, startKey, numKeys);
    List<String> tmp = new ArrayList<String>(Arrays.asList(args));
    tmp.add(HIPHEN + LoadTestTool.OPT_GENERATOR);
    StringBuilder sb = new StringBuilder(LoadTestDataGeneratorWithVisibilityLabels.class.getName());
    sb.append(COLON);
    sb.append(asCommaSeperatedString(VISIBILITY_EXPS));
    sb.append(COLON);
    String authorizationsStr = AUTHS.toString();
    sb.append(authorizationsStr.substring(1, authorizationsStr.length() - 1));
    tmp.add(sb.toString());
    return tmp.toArray(new String[tmp.size()]);
  }

  private static String asCommaSeperatedString(String[] list) {
    StringBuilder sb = new StringBuilder();
    for (String item : list) {
      sb.append(item);
      sb.append(COMMA);
    }
    if (sb.length() > 0) {
      // Remove the trailing ,
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }
  
  private void addLabels() throws Exception {
    try {
      VisibilityClient.addLabels(util.getConnection(), LABELS);
      VisibilityClient.setAuths(util.getConnection(), LABELS, User.getCurrent().getName());
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}
