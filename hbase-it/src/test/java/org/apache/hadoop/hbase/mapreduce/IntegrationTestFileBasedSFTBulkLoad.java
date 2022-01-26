/**
 *
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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;

/**
 * Test Bulk Load and MR on a distributed cluster.
 * With FileBased StorefileTracker enabled.
 * It starts an MR job that creates linked chains
 *
 * The format of rows is like this:
 * Row Key -> Long
 *
 * L:<< Chain Id >> -> Row Key of the next link in the chain
 * S:<< Chain Id >> -> The step in the chain that his link is.
 * D:<< Chain Id >> -> Random Data.
 *
 * All chains start on row 0.
 * All rk's are > 0.
 *
 * After creating the linked lists they are walked over using a TableMapper based Mapreduce Job.
 *
 * There are a few options exposed:
 *
 * hbase.IntegrationTestBulkLoad.chainLength
 * The number of rows that will be part of each and every chain.
 *
 * hbase.IntegrationTestBulkLoad.numMaps
 * The number of mappers that will be run.  Each mapper creates on linked list chain.
 *
 * hbase.IntegrationTestBulkLoad.numImportRounds
 * How many jobs will be run to create linked lists.
 *
 * hbase.IntegrationTestBulkLoad.tableName
 * The name of the table.
 *
 * hbase.IntegrationTestBulkLoad.replicaCount
 * How many region replicas to configure for the table under test.
 */
@Category(IntegrationTests.class)
public class IntegrationTestFileBasedSFTBulkLoad extends IntegrationTestBulkLoad {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestFileBasedSFTBulkLoad.class);

  private static String NUM_MAPS_KEY = "hbase.IntegrationTestBulkLoad.numMaps";
  private static String NUM_IMPORT_ROUNDS_KEY = "hbase.IntegrationTestBulkLoad.numImportRounds";
  private static String NUM_REPLICA_COUNT_KEY = "hbase.IntegrationTestBulkLoad.replicaCount";
  private static int NUM_REPLICA_COUNT_DEFAULT = 1;

  @Test
  public void testFileBasedSFTBulkLoad() throws Exception {
    super.testBulkLoad();
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    util.getConfiguration().set(StoreFileTrackerFactory.TRACKER_IMPL,
      "org.apache.hadoop.hbase.regionserver.storefiletracker.FileBasedStoreFileTracker");
    util.initializeCluster(1);
    int replicaCount = getConf().getInt(NUM_REPLICA_COUNT_KEY, NUM_REPLICA_COUNT_DEFAULT);
    if (LOG.isDebugEnabled() && replicaCount != NUM_REPLICA_COUNT_DEFAULT) {
      LOG.debug("Region Replicas enabled: " + replicaCount);
    }

    // Scale this up on a real cluster
    if (util.isDistributedCluster()) {
      util.getConfiguration().setIfUnset(NUM_MAPS_KEY,
        Integer.toString(util.getAdmin().getRegionServers().size() * 10));
      util.getConfiguration().setIfUnset(NUM_IMPORT_ROUNDS_KEY, "5");
    } else {
      util.startMiniMapReduceCluster();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status =  ToolRunner.run(conf, new IntegrationTestFileBasedSFTBulkLoad(), args);
    System.exit(status);
  }
}
