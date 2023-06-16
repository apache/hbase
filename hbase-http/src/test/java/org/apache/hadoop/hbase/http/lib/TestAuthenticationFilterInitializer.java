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
package org.apache.hadoop.hbase.http.lib;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.http.FilterContainer;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({ MiscTests.class, SmallTests.class })
public class TestAuthenticationFilterInitializer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAuthenticationFilterInitializer.class);

  @Test
  public void testConfiguration() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.http.authentication.foo", "bar");

    conf.set(HttpServer.BIND_ADDRESS, "barhost");

    FilterContainer container = Mockito.mock(FilterContainer.class);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();

        assertEquals("authentication", args[0]);

        assertEquals(AuthenticationFilter.class.getName(), args[1]);

        Map<String, String> conf = (Map<String, String>) args[2];
        assertEquals("/", conf.get("cookie.path"));

        assertEquals("simple", conf.get("type"));
        assertEquals("36000", conf.get("token.validity"));
        assertNull(conf.get("cookie.domain"));
        assertEquals("true", conf.get("simple.anonymous.allowed"));
        assertEquals("HTTP/barhost@LOCALHOST", conf.get("kerberos.principal"));
        assertEquals(System.getProperty("user.home") + "/hadoop.keytab",
          conf.get("kerberos.keytab"));
        assertEquals("bar", conf.get("foo"));

        return null;
      }
    }).when(container).addFilter(any(), any(), any());

    new AuthenticationFilterInitializer().initFilter(container, conf);
  }

}
