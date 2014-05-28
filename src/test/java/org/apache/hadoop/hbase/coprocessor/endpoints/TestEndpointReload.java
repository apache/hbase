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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.CoprocessorAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient.Caller;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ExceptionUtils;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.util.coprocessor.ClassLoaderTestHelper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test of reloading endpoints with online configuration change.
 */
@Category(MediumTests.class)
public class TestEndpointReload {
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static final StringBytes TABLE_NAME = new StringBytes("TER");
  private static final byte[] FAMILY_NAME = Bytes.toBytes("f");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * An endpoint returns 1
   */
  public interface IOne extends IEndpoint {
    int one();
  }

  /**
   * Factory class of IOne
   */
  public static class OneFactory implements IEndpointFactory<IOne> {
    @Override
    public IOne create() {
      return new IOne() {
        @Override
        public void setContext(IEndpointContext context) {
        }

        @Override
        public int one() {
          return 1;
        }
      };
    }

    @Override
    public Class<IOne> getEndpointInterface() {
      return IOne.class;
    }
  }

  private static final String TWO_NAME = "two";
  private static final int TWO_VERSION = 1;
  private static final String TWO_FACTORY_CLASSNAME = "TwoFactory";
  private static final String TWO_ENDPOINT_CLASSNAME = "TwoFactory$ITwo";

  private static File buildEndpointTwoJar() throws Exception {
    String code = "import " + IEndpointFactory.class.getName() + ";\n"
      + "import " + IEndpoint.class.getName() + ";\n"
      + "import " + IEndpointContext.class.getName() + ";\n"
      + "public class TwoFactory implements IEndpointFactory<TwoFactory.ITwo> {\n"
      + "  public interface ITwo extends IEndpoint {\n"
      + "    int two();\n"
      + "  }\n"
      + "  public ITwo create() {\n"
      + "    return new ITwo() {\n"
      + "      public void setContext(IEndpointContext context) {}\n"
      + "      public int two() {\n"
      + "        return 2;\n"
      + "      }\n"
      + "    };\n"
      + "  }\n"
      + "  public Class<ITwo> getEndpointInterface() {\n"
      + "    return ITwo.class;\n"
      + "  }\n"
      + "}";
    return ClassLoaderTestHelper.buildJar(TEST_UTIL.getDFSCluster()
        .getDataDirectory().toString(), "TwoFactory", code);
  }

  private void uploadEndpointTow(File jarFile, String name, int version)
      throws Exception {
    Path localPath =
        TEST_UTIL.getTestDir("TestCoprocessorAdmin-uploadEndpointTow-" + name
            + "-" + version);

    LocalFileSystem lfs = FileSystem.getLocal(TEST_UTIL.getConfiguration());
    lfs.delete(localPath, true);

    lfs.mkdirs(localPath);
    lfs.copyFromLocalFile(false, new Path(jarFile.toString()), new Path(
        localPath, jarFile.getName()));

    JsonFactory f = new JsonFactory();
    try (OutputStream out =
        lfs.create(new Path(localPath, CoprocessorHost.CONFIG_JSON))) {
      JsonGenerator g = f.createJsonGenerator(out);
      g.writeStartObject();

      g.writeStringField(CoprocessorHost.COPROCESSOR_JSON_NAME_FIELD, name);

      g.writeNumberField(CoprocessorHost.COPROCESSOR_JSON_VERSION_FIELD,
          version);

      g.writeArrayFieldStart(CoprocessorHost.COPROCESSOR_JSON_LOADED_CLASSES_FIELD);
      g.writeString(TWO_FACTORY_CLASSNAME);
      g.writeEndArray();
      g.writeEndObject();
      g.close();
    }

    new CoprocessorAdmin(TEST_UTIL.getConfiguration()).upload(localPath, true);
  }

  @SuppressWarnings("unchecked")
  private void checkEndpionts(IEndpointClient cp, File jarFile, boolean one,
      boolean two) throws Exception {
    try {
      Map<HRegionInfo, Integer> res = cp.coprocessorEndpoint(IOne.class, null,
          null, new Caller<IOne, Integer>() {
        @Override
        public Integer call(IOne client) throws IOException {
          return client.one();
        }
      });
      if (!one) {
        Assert.fail("Should not have IOne endpoint");
      }
      for (int vl : res.values()) {
        Assert.assertEquals("vl", 1, vl);
      }
    } catch (NoSuchEndpointException e) {
      if (one) {
        throw e;
      }
    }

    try {
      @SuppressWarnings("resource")
      ClassLoader cl = new URLClassLoader(new URL[]{jarFile.toURI().toURL()});
      Class<IEndpoint> ITwoClass =
          (Class<IEndpoint>) cl.loadClass(TWO_ENDPOINT_CLASSNAME);

      Map<HRegionInfo, Integer> res = cp.coprocessorEndpoint(ITwoClass, null,
          null, new Caller<IEndpoint, Integer>() {
        @Override
        public Integer call(IEndpoint client) throws IOException {
          try {
            Method mth = client.getClass().getMethod("two");
            return (Integer) mth.invoke(client);
          } catch (Throwable e) {
            throw ExceptionUtils.toIOException(e);
          }
        }
      });
      if (!two) {
        Assert.fail("Should not have ITwo endpoint");
      }
      for (int vl : res.values()) {
        Assert.assertEquals("vl", 2, vl);
      }
    } catch (NoSuchEndpointException e) {
      if (two) {
        throw e;
      }
    }
  }

  /**
   * Testing the reloading of endpoints after configuration is changed.
   * Configuration is changed and configurationManager is used to perform the
   * change, then checkEndpionts is called to check the status.
   */
  @Test(timeout = 180000)
  public void testReload() throws Exception {
    HTableInterface table = TEST_UTIL.createTable(TABLE_NAME, FAMILY_NAME);
    IEndpointClient cp = (IEndpointClient) table;
    Configuration conf = TEST_UTIL.getConfiguration();

    File jarFile = buildEndpointTwoJar();
    uploadEndpointTow(jarFile, TWO_NAME, TWO_VERSION);

    // none
    checkEndpionts(cp, jarFile, false, false);

    // one
    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        OneFactory.class.getName());
    HRegionServer.configurationManager.notifyAllObservers(conf);
    checkEndpionts(cp, jarFile, true, false);

    // two
    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        EndpointLoader.genDfsEndpointEntry(TWO_NAME, TWO_VERSION));
    HRegionServer.configurationManager.notifyAllObservers(conf);
    checkEndpionts(cp, jarFile, false, true);

    // one+two
    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        OneFactory.class.getName(),
        EndpointLoader.genDfsEndpointEntry(TWO_NAME, TWO_VERSION));
    HRegionServer.configurationManager.notifyAllObservers(conf);
    checkEndpionts(cp, jarFile, true, true);
  }

}
