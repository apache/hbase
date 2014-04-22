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

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient.Caller;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test of reloading endpoints with online configuration change.
 */
public class TestEndpointReload {
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static final StringBytes TABLE_NAME = new StringBytes("TER");
  private static final byte[] FAMILY_NAME = Bytes.toBytes("f");

  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    conf.set(HBaseTestingUtility.FS_TYPE_KEY, HBaseTestingUtility.FS_TYPE_LFS);

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

  /**
   * An endpoint returns 2
   */
  public interface ITwo extends IEndpoint {
    int two();
  }

  /**
   * Factory class of ITwo
   */
  public static class TwoFactory implements IEndpointFactory<ITwo> {
    @Override
    public ITwo create() {
      return new ITwo() {
        @Override
        public void setContext(IEndpointContext context) {
        }

        @Override
        public int two() {
          return 2;
        }
      };
    }

    @Override
    public Class<ITwo> getEndpointInterface() {
      return ITwo.class;
    }
  }

  private void checkEndpionts(IEndpointClient cp, boolean one, boolean two)
      throws Exception {
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
      Map<HRegionInfo, Integer> res = cp.coprocessorEndpoint(ITwo.class, null,
          null, new Caller<ITwo, Integer>() {
        @Override
        public Integer call(ITwo client) throws IOException {
          return client.two();
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

    // none
    checkEndpionts(cp, false, false);

    // one
    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        OneFactory.class.getName());
    HRegionServer.configurationManager.notifyAllObservers(conf);
    checkEndpionts(cp, true, false);

    // two
    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        TwoFactory.class.getName());
    HRegionServer.configurationManager.notifyAllObservers(conf);
    checkEndpionts(cp, false, true);

    // one+two
    conf.setStrings(EndpointLoader.FACTORY_CLASSES_KEY,
        OneFactory.class.getName(), TwoFactory.class.getName());
    HRegionServer.configurationManager.notifyAllObservers(conf);
    checkEndpionts(cp, true, true);
  }

}
