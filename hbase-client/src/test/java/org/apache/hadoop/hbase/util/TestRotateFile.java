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
package org.apache.hadoop.hbase.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

@Category({ MiscTests.class, SmallTests.class })
public class TestRotateFile {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRotateFile.class);

  private static HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static FileSystem FS;

  private Path dir;

  private RotateFile rotateFile;

  @Rule
  public final TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    FS = FileSystem.get(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() {
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    dir = UTIL.getDataTestDir(name.getMethodName());
    if (!FS.mkdirs(dir)) {
      throw new IOException("Can not create dir " + dir);
    }
    rotateFile = new RotateFile(FS, dir, name.getMethodName(), 1024);
    assertNull(rotateFile.read());
  }

  @Test
  public void testSimpleReadWrite() throws IOException {
    for (int i = 0; i < 10; i++) {
      rotateFile.write(Bytes.toBytes(i));
      assertEquals(i, Bytes.toInt(rotateFile.read()));
    }
    rotateFile.delete();
    assertNull(rotateFile.read());
  }

  @Test
  public void testCompareTimestamp() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    rotateFile.write(Bytes.toBytes(10));
    Path file = FS.listStatus(dir)[0].getPath();
    rotateFile.write(Bytes.toBytes(100));

    // put a fake file with a less timestamp there
    RotateFile.write(FS, file, now - 1, Bytes.toBytes(10));
    assertEquals(100, Bytes.toInt(rotateFile.read()));

    // put a fake file with a greater timestamp there
    RotateFile.write(FS, file, EnvironmentEdgeManager.currentTime() + 100, Bytes.toBytes(10));
    assertEquals(10, Bytes.toInt(rotateFile.read()));
  }

  @Test
  public void testMaxFileSize() throws IOException {
    assertThrows(IOException.class, () -> rotateFile.write(new byte[1025]));
    // put a file greater than max file size
    rotateFile.write(Bytes.toBytes(10));
    Path file = FS.listStatus(dir)[0].getPath();
    RotateFile.write(FS, file, EnvironmentEdgeManager.currentTime(), new byte[1025]);
    assertThrows(IOException.class, () -> rotateFile.read());
  }

  @Test
  public void testNotEnoughData() throws IOException {
    rotateFile.write(Bytes.toBytes(10));
    assertEquals(10, Bytes.toInt(rotateFile.read()));
    // remove the last byte
    Path file = FS.listStatus(dir)[0].getPath();
    byte[] data;
    try (FSDataInputStream in = FS.open(file)) {
      data = ByteStreams.toByteArray(in);
    }
    try (FSDataOutputStream out = FS.create(file, true)) {
      out.write(data, 0, data.length - 1);
    }
    // should hit EOF so read nothing
    assertNull(rotateFile.read());
  }

  @Test
  public void testChecksumMismatch() throws IOException {
    rotateFile.write(Bytes.toBytes(10));
    assertEquals(10, Bytes.toInt(rotateFile.read()));
    // mess up one byte
    Path file = FS.listStatus(dir)[0].getPath();
    byte[] data;
    try (FSDataInputStream in = FS.open(file)) {
      data = ByteStreams.toByteArray(in);
    }
    data[4]++;
    try (FSDataOutputStream out = FS.create(file, true)) {
      out.write(data, 0, data.length);
    }
    // should get checksum mismatch
    IOException error = assertThrows(IOException.class, () -> rotateFile.read());
    assertThat(error.getMessage(), startsWith("Checksum mismatch"));
  }
}
