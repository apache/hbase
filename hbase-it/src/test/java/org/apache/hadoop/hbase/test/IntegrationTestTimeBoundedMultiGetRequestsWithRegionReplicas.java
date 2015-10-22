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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Extends {@link IntegrationTestTimeBoundedRequestsWithRegionReplicas} for multi-gets
 * Besides the options already talked about in IntegrationTestTimeBoundedRequestsWithRegionReplicas
 * the addition options here are:
 * <pre>
 * -DIntegrationTestTimeBoundedMultiGetRequestsWithRegionReplicas.multiget_batchsize=100
 * -DIntegrationTestTimeBoundedMultiGetRequestsWithRegionReplicas.num_regions_per_server=5
 * </pre>
 * The multiget_batchsize when set to 1 will issue normal GETs.
 * The num_regions_per_server argument indirectly impacts the region size (for a given number of
 * num_keys_per_server). That in conjunction with multiget_batchsize would have different behaviors
 * - the batch of gets goes to the same region or to multiple regions.
 */
@Category(IntegrationTests.class)
public class IntegrationTestTimeBoundedMultiGetRequestsWithRegionReplicas
    extends IntegrationTestTimeBoundedRequestsWithRegionReplicas {

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    List<String> args = Lists.newArrayList(super.getArgsForLoadTestTool(
      mode, modeSpecificArg, startKey, numKeys));
    String clazz = this.getClass().getSimpleName();
    args.add("-" + LoadTestTool.OPT_MULTIGET);
    args.add(conf.get(String.format("%s.%s", clazz, LoadTestTool.OPT_MULTIGET), "100"));

    args.add("-" + LoadTestTool.OPT_NUM_REGIONS_PER_SERVER);
    args.add(conf.get(String.format("%s.%s", clazz, LoadTestTool.OPT_NUM_REGIONS_PER_SERVER),
        Integer.toString(LoadTestTool.DEFAULT_NUM_REGIONS_PER_SERVER)));

    return args.toArray(new String[args.size()]);
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf,
        new IntegrationTestTimeBoundedMultiGetRequestsWithRegionReplicas(), args);
    System.exit(ret);
  }
}
