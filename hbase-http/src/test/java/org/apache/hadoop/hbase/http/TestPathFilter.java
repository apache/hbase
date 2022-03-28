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
import java.util.Set;
import java.util.TreeSet;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestPathFilter extends HttpServerFunctionalTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPathFilter.class);

  private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
  private static final Set<String> RECORDS = new TreeSet<>();

  /** A very simple filter that records accessed uri's */
  static public class RecordingFilter implements Filter {
    private FilterConfig filterConfig = null;

    @Override
    public void init(FilterConfig filterConfig) {
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

      String uri = ((HttpServletRequest)request).getRequestURI();
      LOG.info("filtering " + uri);
      RECORDS.add(uri);
      chain.doFilter(request, response);
    }

    /** Configuration for RecordingFilter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {}

      @Override
      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("recording", RecordingFilter.class.getName(), null);
      }
    }
  }

  @Test
  public void testPathSpecFilters() throws Exception {
    Configuration conf = new Configuration();

    //start an http server with CountingFilter
    conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY,
        RecordingFilter.Initializer.class.getName());
    String[] pathSpecs = { "/path", "/path/*" };
    HttpServer http = createTestServer(conf, pathSpecs);
    http.start();

    final String baseURL = "/path";
    final String baseSlashURL = "/path/";
    final String addedURL = "/path/nodes";
    final String addedSlashURL = "/path/nodes/";
    final String longURL = "/path/nodes/foo/job";
    final String rootURL = "/";
    final String allURL = "/*";

    final String[] filteredUrls = { baseURL, baseSlashURL, addedURL, addedSlashURL, longURL };
    final String[] notFilteredUrls = {rootURL, allURL};

    // access the urls and verify our paths specs got added to the
    // filters
    final String prefix = "http://"
        + NetUtils.getHostPortString(http.getConnectorAddress(0));
    try {
      for (String filteredUrl : filteredUrls) {
        access(prefix + filteredUrl);
      }
      for (String notFilteredUrl : notFilteredUrls) {
        access(prefix + notFilteredUrl);
      }
    } finally {
      http.stop();
    }

    LOG.info("RECORDS = " + RECORDS);

    //verify records
    for (String filteredUrl : filteredUrls) {
      assertTrue(RECORDS.remove(filteredUrl));
    }
    assertTrue(RECORDS.isEmpty());
  }
}
