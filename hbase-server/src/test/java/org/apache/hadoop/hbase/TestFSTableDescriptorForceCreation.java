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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MiscTests.class, SmallTests.class})
public class TestFSTableDescriptorForceCreation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSTableDescriptorForceCreation.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @Test
  public void testShouldCreateNewTableDescriptorIfForcefulCreationIsFalse()
      throws IOException {
    final String name = this.name.getMethodName();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors fstd = new FSTableDescriptors(fs, rootdir);

    assertTrue("Should create new table descriptor",
      fstd.createTableDescriptor(TableDescriptorBuilder.newBuilder(TableName.valueOf(name)).build(), false));
  }

  @Test
  public void testShouldNotCreateTheSameTableDescriptorIfForcefulCreationIsFalse()
      throws IOException {
    final String name = this.name.getMethodName();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detritus laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors fstd = new FSTableDescriptors(fs, rootdir);
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(name)).build();
    fstd.update(htd);
    assertFalse("Should not create new table descriptor", fstd.createTableDescriptor(htd, false));
  }

  @Test
  public void testShouldAllowForcefulCreationOfAlreadyExistingTableDescriptor()
      throws Exception {
    final String name = this.name.getMethodName();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    FSTableDescriptors fstd = new FSTableDescriptors(fs, rootdir);
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(name)).build();
    fstd.createTableDescriptor(htd, false);
    assertTrue("Should create new table descriptor",
        fstd.createTableDescriptor(htd, true));
  }

}

