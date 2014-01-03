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

import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.LoadTestDataGeneratorWithTags;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)
public class IntegrationTestIngestWithTags extends IntegrationTestIngest {

  private static final char COLON = ':';

  private int minTagsPerKey = 1, maxTagsPerKey = 10;
  private int minTagLength = 16, maxTagLength = 512;

  @Override
  public void setUpCluster() throws Exception {
    getTestingUtil(conf).getConfiguration().setInt(HFile.FORMAT_VERSION_KEY, 3);
    super.setUpCluster();
  }

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    String[] args = super.getArgsForLoadTestTool(mode, modeSpecificArg, startKey, numKeys);
    List<String> tmp = new ArrayList<String>(Arrays.asList(args));
    // LoadTestDataGeneratorWithTags:minNumTags:maxNumTags:minTagLength:maxTagLength
    tmp.add(HIPHEN + LoadTestTool.OPT_GENERATOR);
    StringBuilder sb = new StringBuilder(LoadTestDataGeneratorWithTags.class.getName());
    sb.append(COLON);
    sb.append(minTagsPerKey);
    sb.append(COLON);
    sb.append(maxTagsPerKey);
    sb.append(COLON);
    sb.append(minTagLength);
    sb.append(COLON);
    sb.append(maxTagLength);
    tmp.add(sb.toString());
    return tmp.toArray(new String[tmp.size()]);
  }
}
