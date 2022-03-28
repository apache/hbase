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
package org.apache.hadoop.hbase.http;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestServletFilter extends HttpServerFunctionalTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServletFilter.class);

  private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
  private static volatile String uri = null;

  /** A very simple filter which record the uri filtered. */
  static public class SimpleFilter implements Filter {
    private FilterConfig filterConfig = null;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
      this.filterConfig = filterConfig;
    }

    @Override
    public void destroy() {
      this.filterConfig = null;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      if (filterConfig == null) {
        return;
      }

      uri = ((HttpServletRequest)request).getRequestURI();
      LOG.info("filtering " + uri);
      chain.doFilter(request, response);
    }

    /** Configuration for the filter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {}

      @Override
      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("simple", SimpleFilter.class.getName(), null);
      }
    }
  }

  private static void assertExceptionContains(String string, Throwable t) {
    String msg = t.getMessage();
    Assert.assertTrue(
        "Expected to find '" + string + "' but got unexpected exception:"
        + StringUtils.stringifyException(t), msg.contains(string));
  }

  @Test
  @Ignore
  //From stack
  // Its a 'foreign' test, one that came in from hadoop when we copy/pasted http
  // It's second class. Could comment it out if only failing test (as per @nkeywal – sort of)
  public void testServletFilter() throws Exception {
    Configuration conf = new Configuration();

    //start an http server with CountingFilter
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
        SimpleFilter.Initializer.class.getName());
    HttpServer http = createTestServer(conf);
    http.start();

    final String fsckURL = "/fsck";
    final String stacksURL = "/stacks";
    final String ajspURL = "/a.jsp";
    final String logURL = "/logs/a.log";
    final String hadooplogoURL = "/static/hadoop-logo.jpg";

    final String[] urls = {fsckURL, stacksURL, ajspURL, logURL, hadooplogoURL};
    final Random rand = ThreadLocalRandom.current();
    final int[] sequence = new int[50];

    //generate a random sequence and update counts
    for(int i = 0; i < sequence.length; i++) {
      sequence[i] = rand.nextInt(urls.length);
    }

    //access the urls as the sequence
    final String prefix = "http://"
        + NetUtils.getHostPortString(http.getConnectorAddress(0));
    try {
      for (int aSequence : sequence) {
        access(prefix + urls[aSequence]);

        //make sure everything except fsck get filtered
        if (aSequence == 0) {
          assertNull(uri);
        } else {
          assertEquals(urls[aSequence], uri);
          uri = null;
        }
      }
    } finally {
      http.stop();
    }
  }

  static public class ErrorFilter extends SimpleFilter {
    @Override
    public void init(FilterConfig arg0) throws ServletException {
      throw new ServletException("Throwing the exception from Filter init");
    }

    /** Configuration for the filter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {
      }

      @Override
      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("simple", ErrorFilter.class.getName(), null);
      }
    }
  }

  @Test
  public void testServletFilterWhenInitThrowsException() throws Exception {
    Configuration conf = new Configuration();
    // start an http server with ErrorFilter
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
        ErrorFilter.Initializer.class.getName());
    HttpServer http = createTestServer(conf);
    try {
      http.start();
      fail("expecting exception");
    } catch (IOException e) {
      assertExceptionContains("Problem starting http server", e);
    }
  }

  /**
   * Similar to the above test case, except that it uses a different API to add the
   * filter. Regression test for HADOOP-8786.
   */
  @Test
  public void testContextSpecificServletFilterWhenInitThrowsException()
      throws Exception {
    Configuration conf = new Configuration();
    HttpServer http = createTestServer(conf);
    HttpServer.defineFilter(http.webAppContext,
        "ErrorFilter", ErrorFilter.class.getName(),
        null, null);
    try {
      http.start();
      fail("expecting exception");
    } catch (IOException e) {
      assertExceptionContains("Unable to initialize WebAppContext", e);
    }
  }
}
