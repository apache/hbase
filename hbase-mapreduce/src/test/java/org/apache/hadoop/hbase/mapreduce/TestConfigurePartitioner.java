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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MapReduceTests.class, MediumTests.class })
public class TestConfigurePartitioner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConfigurePartitioner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestConfigurePartitioner.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    UTIL.startMiniDFSCluster(1);
  }

  @After
  public void tearDown() throws IOException {
    UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testConfigurePartitioner() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // Create a user who is not the current user
    String fooUserName = "foo1234";
    String fooGroupName = "group1";
    UserGroupInformation ugi =
      UserGroupInformation.createUserForTesting(fooUserName, new String[] { fooGroupName });
    // Get user's home directory
    Path fooHomeDirectory = ugi.doAs(new PrivilegedAction<Path>() {
      @Override
      public Path run() {
        try (FileSystem fs = FileSystem.get(conf)) {
          return fs.makeQualified(fs.getHomeDirectory());
        } catch (IOException ioe) {
          LOG.error("Failed to get foo's home directory", ioe);
        }
        return null;
      }
    });
    // create the home directory and chown
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(fooHomeDirectory);
    fs.setOwner(fooHomeDirectory, fooUserName, fooGroupName);

    Job job = Mockito.mock(Job.class);
    Mockito.doReturn(conf).when(job).getConfiguration();
    ImmutableBytesWritable writable = new ImmutableBytesWritable();
    List<ImmutableBytesWritable> splitPoints = new ArrayList<ImmutableBytesWritable>();
    splitPoints.add(writable);

    ugi.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          HFileOutputFormat2.configurePartitioner(job, splitPoints, false);
        } catch (IOException ioe) {
          LOG.error("Failed to configure partitioner", ioe);
        }
        return null;
      }
    });
    // verify that the job uses TotalOrderPartitioner
    verify(job).setPartitionerClass(TotalOrderPartitioner.class);
    // verify that TotalOrderPartitioner.setPartitionFile() is called.
    String partitionPathString = conf.get("mapreduce.totalorderpartitioner.path");
    assertNotNull(partitionPathString);
    // Make sure the partion file is in foo1234's home directory, and that
    // the file exists.
    assertTrue(partitionPathString.startsWith(fooHomeDirectory.toString()));
    assertTrue(fs.exists(new Path(partitionPathString)));
  }
}
