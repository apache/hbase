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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Used to confirm that it is OK to overwrite a file which is being written currently.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestOverwriteFileUnderConstruction extends AsyncFSTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOverwriteFileUnderConstruction.class);

  private static FileSystem FS;

  @Rule
  public final TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    startMiniDFSCluster(3);
    FS = CLUSTER.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    shutdownMiniDFSCluster();
  }

  @Test
  public void testNotOverwrite() throws IOException {
    Path file = new Path("/" + name.getMethodName());
    try (FSDataOutputStream out1 = FS.create(file)) {
      try {
        FS.create(file, false);
        fail("Should fail as there is a file with the same name which is being written");
      } catch (RemoteException e) {
        // expected
        assertThat(e.unwrapRemoteException(), instanceOf(AlreadyBeingCreatedException.class));
      }
    }
  }

  @Test
  public void testOverwrite() throws IOException {
    Path file = new Path("/" + name.getMethodName());
    FSDataOutputStream out1 = FS.create(file);
    FSDataOutputStream out2 = FS.create(file, true);
    out1.write(2);
    out2.write(1);
    try {
      out1.close();
      // a successful close is also OK for us so no assertion here, we just need to confirm that the
      // data in the file are correct.
    } catch (FileNotFoundException fnfe) {
      // hadoop3 throws one of these.
    } catch (RemoteException e) {
      // expected
      assertThat(e.unwrapRemoteException(), instanceOf(LeaseExpiredException.class));
    }
    out2.close();
    try (FSDataInputStream in = FS.open(file)) {
      assertEquals(1, in.read());
      assertEquals(-1, in.read());
    }
  }
}
