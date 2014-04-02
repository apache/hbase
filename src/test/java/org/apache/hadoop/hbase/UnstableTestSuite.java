/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.client.TestFromClientSide;
import org.apache.hadoop.hbase.client.TestScannerTimeout;
import org.apache.hadoop.hbase.client.TestServerConfigFromClient;
import org.apache.hadoop.hbase.mapreduce.TestHFileOutputFormat;
import org.apache.hadoop.hbase.master.TestLogSplitOnMasterFailover;
import org.apache.hadoop.hbase.master.TestRSLivenessOnMasterFailover;
import org.apache.hadoop.hbase.master.TestRegionPlacement;
import org.apache.hadoop.hbase.master.TestRegionStateOnMasterFailure;
import org.apache.hadoop.hbase.regionserver.TestHRegionCloseRetry;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerFileSystemFailure;
import org.apache.hadoop.hbase.regionserver.metrics.TestThriftMetrics;
import org.apache.hadoop.hbase.regionserver.wal.TestLogRolling;
import org.apache.hadoop.hbase.regionserver.wal.TestWALReplay;
import org.apache.hadoop.hbase.replication.regionserver.TestReplicationSink;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestProcessBasedCluster;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestHRegionCloseRetry.class, TestReplicationSink.class,
    TestLogRolling.class, TestHFileOutputFormat.class,
    TestProcessBasedCluster.class, TestRegionStateOnMasterFailure.class,
    TestScannerTimeout.class, TestWALReplay.class, TestThriftMetrics.class,
    TestRegionPlacement.class, TestServerConfigFromClient.class,
    TestRSLivenessOnMasterFailover.class, TestLogSplitOnMasterFailover.class,
    TagRunner.class, TestHRegionServerFileSystemFailure.class,
    TestFromClientSide.class })
public class UnstableTestSuite {
}
