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
package org.apache.hadoop.hbase.backup;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MediumTests.TAG)
public class TestBackupSmallTests extends TestBackupBase {

  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] {});
  private static final String PERMISSION_TEST_PATH = Path.SEPARATOR + "permissionUT";

  @Test
  public void testBackupPathIsAccessible() throws Exception {
    Path path = new Path(PERMISSION_TEST_PATH);
    FileSystem fs = FileSystem.get(TEST_UTIL.getConnection().getConfiguration());
    fs.mkdirs(path);
  }

  @Test
  public void testBackupPathIsNotAccessible() throws Exception {
    assertThrows(IOException.class, () -> {
      Path path = new Path(PERMISSION_TEST_PATH);
      FileSystem rootFs = FileSystem.get(TEST_UTIL.getConnection().getConfiguration());
      rootFs.mkdirs(path.getParent());
      rootFs.setPermission(path.getParent(), FsPermission.createImmutable((short) 000));
      FileSystem fs =
        DFSTestUtil.getFileSystemAs(DIANA, TEST_UTIL.getConnection().getConfiguration());
      fs.mkdirs(path);
    });
  }
}
