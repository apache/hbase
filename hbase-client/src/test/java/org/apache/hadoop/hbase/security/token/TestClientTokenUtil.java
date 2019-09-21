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
package org.apache.hadoop.hbase.security.token;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

@Category(SmallTests.class)
public class TestClientTokenUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientTokenUtil.class);

  private URLClassLoader cl;

  @Before
  public void setUp() {
    URL urlPU = ProtobufUtil.class.getProtectionDomain().getCodeSource().getLocation();
    URL urlCTU = ClientTokenUtil.class.getProtectionDomain().getCodeSource().getLocation();
    cl = new URLClassLoader(new URL[] { urlPU, urlCTU }, getClass().getClassLoader());
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(cl, true);
  }

  @Test
  public void testObtainToken() throws Exception {
    Throwable injected = new com.google.protobuf.ServiceException("injected");

    Class<?> clientTokenUtil = cl.loadClass(ClientTokenUtil.class.getCanonicalName());
    Field shouldInjectFault = clientTokenUtil.getDeclaredField("injectedException");
    shouldInjectFault.setAccessible(true);
    shouldInjectFault.set(null, injected);

    try {
      ClientTokenUtil.obtainToken((Connection)null);
      fail("Should have injected exception.");
    } catch (IOException e) {
      Throwable t = e;
      boolean serviceExceptionFound = false;
      while ((t = t.getCause()) != null) {
        if (t == injected) { // reference equality
          serviceExceptionFound = true;
          break;
        }
      }
      if (!serviceExceptionFound) {
        throw e; // wrong exception, fail the test
      }
    }

    Boolean loaded = (Boolean) cl.loadClass(ProtobufUtil.class.getCanonicalName())
      .getDeclaredMethod("isClassLoaderLoaded").invoke(null);
    assertFalse("Should not have loaded DynamicClassLoader", loaded);
  }
}
