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
package org.apache.hadoop.hbase.coprocessor;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.coprocessor.observers.BaseRegionObserver;
import org.apache.hadoop.hbase.util.coprocessor.ClassLoaderTestHelper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testcases for CoprocessorAdmin.
 */
public class TestCoprocessorAdmin {
  private static final HBaseTestingUtility TEST_UTILS =
      new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTILS.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTILS.shutdownMiniCluster();
  }

  static File buildCoprocessorJar(String className) throws Exception {
    String code =
        "import " + BaseRegionObserver.class.getName() + ";\n"
            + "public class " + className + " extends BaseRegionObserver {}";
    return ClassLoaderTestHelper.buildJar(TEST_UTILS.getDFSCluster()
        .getDataDirectory().toString(), className, code);
  }

  private void prepareFolderToUpload(Path dstPath, File jarFile, String name,
      int version, String className) throws IOException {
    LocalFileSystem lfs = FileSystem.getLocal(TEST_UTILS.getConfiguration());
    lfs.delete(dstPath, true);

    lfs.mkdirs(dstPath);
    lfs.copyFromLocalFile(false, new Path(jarFile.toString()), new Path(
        dstPath, jarFile.getName()));

    JsonFactory f = new JsonFactory();
    try (OutputStream out =
        lfs.create(new Path(dstPath, CoprocessorHost.CONFIG_JSON))) {
      JsonGenerator g = f.createJsonGenerator(out);
      g.writeStartObject();

      g.writeStringField(CoprocessorHost.COPROCESSOR_JSON_NAME_FIELD, name);

      g.writeNumberField(CoprocessorHost.COPROCESSOR_JSON_VERSION_FIELD, version);

      g.writeArrayFieldStart(CoprocessorHost.COPROCESSOR_JSON_LOADED_CLASSES_FIELD);
      g.writeString(className);
      g.writeEndArray();
      g.writeEndObject();
      g.close();
    }
  }

  @Test
  public void testUploadBasic() throws Exception {
    final String CP_NAME = "cp_basic";
    final int CP_VERSION = 1;
    final String CP_CLASSNAME = "TestCPUploadBasic";

    // Create jar file
    File localJar = buildCoprocessorJar(CP_CLASSNAME);
    // Preparing the local folder and config files.
    Path cpLocalPath =
        TEST_UTILS.getTestDir("TestCoprocessorAdmin-testUploadBasic");
    prepareFolderToUpload(cpLocalPath, localJar, CP_NAME, CP_VERSION,
        CP_CLASSNAME);

    CoprocessorAdmin admin =
        new CoprocessorAdmin(TEST_UTILS.getConfiguration());

    // Upload the coprocessor.
    admin.upload(cpLocalPath, false);

    // Check files.
    FileSystem fs = TEST_UTILS.getTestFileSystem();
    String cpRoot = CoprocessorHost.getCoprocessorDfsRoot(
        TEST_UTILS.getConfiguration());

    String cpDfsPath = CoprocessorHost.getCoprocessorPath(cpRoot, CP_NAME,
        CP_VERSION);

    Assert.assertTrue("Coprocessor path on dfs is not a folder", fs
        .getFileStatus(new Path(cpDfsPath)).isDir());
    Assert.assertTrue("Jar file not exists",
        fs.exists(new Path(cpDfsPath, localJar.getName())));
    Assert.assertTrue("JSon file not exists",
        fs.exists(new Path(cpDfsPath, CoprocessorHost.CONFIG_JSON)));
  }

  /**
   * In this case, a file on the destination path is created. Set force to true
   * to remove that file before uploading.
   */
  @Test
  public void testUploadForce() throws Exception {
    final String CP_NAME = "cp_force";
    final int CP_VERSION = 1;
    final String CP_CLASSNAME = "TestCPUploadForce";

    // Create jar file
    File localJar = buildCoprocessorJar(CP_CLASSNAME);
    // Preparing the local folder and config files.
    Path cpLocalPath =
        TEST_UTILS.getTestDir("TestCoprocessorAdmin-testUploadForce");
    prepareFolderToUpload(cpLocalPath, localJar, CP_NAME, CP_VERSION,
        CP_CLASSNAME);

    CoprocessorAdmin admin =
        new CoprocessorAdmin(TEST_UTILS.getConfiguration());

    String cpRoot =
        CoprocessorHost.getCoprocessorDfsRoot(TEST_UTILS.getConfiguration());

    String cpDfsPath =
        CoprocessorHost.getCoprocessorPath(cpRoot, CP_NAME, CP_VERSION);

    // Create an empty file on cpDfsPath.
    FileSystem fs = TEST_UTILS.getTestFileSystem();
    fs.create(new Path(cpDfsPath)).close();
    Assert.assertFalse("Creating file " + cpDfsPath + " failed!", fs
        .getFileStatus(new Path(cpDfsPath)).isDir());

    // Upload the coprocessor.
    admin.upload(cpLocalPath, true);

    // Check files.
    Assert.assertTrue("Coprocessor path on dfs is not a folder", fs
        .getFileStatus(new Path(cpDfsPath)).isDir());
    Assert.assertTrue("Jar file not exists",
        fs.exists(new Path(cpDfsPath, localJar.getName())));
    Assert.assertTrue("JSon file not exists",
        fs.exists(new Path(cpDfsPath, CoprocessorHost.CONFIG_JSON)));
  }

}
