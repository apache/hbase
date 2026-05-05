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
package org.apache.hadoop.hbase.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestProfileServlet {

  // ---- parseProfileRequest ----

  @Test
  public void testParseProfileRequestDefaults() {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(Collections.emptyMap(),
      "pid", null, "duration", null, "output", null, "event", null,
      "interval", null, "jstackdepth", null, "bufsize", null,
      "width", null, "height", null, "minwidth", null, "refreshDelay", null);

    ProfileServlet.ProfileRequest parsed = servlet.parseProfileRequest(req);
    assertNull(parsed.getPid());
    assertEquals(10, parsed.getDuration());
    assertEquals(ProfileServlet.Event.CPU, parsed.getEvent());
    assertEquals(ProfileServlet.Output.HTML, parsed.getOutput());
    assertFalse(parsed.isThread());
    assertFalse(parsed.isSimple());
    assertFalse(parsed.isReverse());
  }

  @Test
  public void testParseProfileRequestAllOptions() {
    Map<String, String[]> flags = new HashMap<>();
    flags.put("thread", new String[] { "" });
    flags.put("simple", new String[] { "" });
    flags.put("reverse", new String[] { "" });

    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(flags,
      "pid", "42", "duration", "60", "output", "tree", "event", "alloc",
      "interval", "1000", "jstackdepth", "256", "bufsize", "100000",
      "width", "1200", "height", "16", "minwidth", "0.5", "refreshDelay", "3");

    ProfileServlet.ProfileRequest parsed = servlet.parseProfileRequest(req);
    assertEquals(42, parsed.getPid());
    assertEquals(60, parsed.getDuration());
    assertEquals(ProfileServlet.Output.TREE, parsed.getOutput());
    assertEquals(ProfileServlet.Event.ALLOC, parsed.getEvent());
    assertEquals(1000L, parsed.getInterval());
    assertEquals(256, parsed.getJstackDepth());
    assertEquals(100000L, parsed.getBufsize());
    assertEquals(1200, parsed.getWidth());
    assertEquals(16, parsed.getHeight());
    assertEquals(0.5, parsed.getMinwidth());
    assertEquals(3, parsed.getRefreshDelay());
    assertTrue(parsed.isThread());
    assertTrue(parsed.isSimple());
    assertTrue(parsed.isReverse());
  }

  // ---- doGet ----

  @Test
  public void testDoGetSetsRefreshHeaderAndCallsBackend() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), Mockito.any())).thenReturn("OK");

    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(),
      "pid", null, "duration", "1", "refreshDelay", "2",
      "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);

    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    servlet.doGet(req, resp);

    Mockito.verify(mockBackend).executeStart(Mockito.any(), Mockito.any());
    Mockito.verify(resp).setStatus(HttpServletResponse.SC_ACCEPTED);

    ArgumentCaptor<String> refreshCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(resp).setHeader(Mockito.eq("Refresh"), refreshCaptor.capture());
    assertTrue(refreshCaptor.getValue().startsWith("3;"));
    assertTrue(refreshCaptor.getValue().contains("/prof-output-hbase/"));
  }

  // ---- isAvailable / getAsyncProfilerHome ----

  @Test
  public void testIsAvailableDetectReturnsBackendWhenLibraryPresent() {
    // async-profiler is on the test classpath (compile-time optional dep present in tests),
    // so detect() returns LibraryBackend even with null home.
    assertNotNull(ProfilerBackend.detect(null));
  }

  @Test
  public void testGetAsyncProfilerHomeSystemProperty() {
    String key = "async.profiler.home";
    String prev = System.getProperty(key);
    try {
      System.setProperty(key, "/tmp/fake-profiler");
      assertEquals("/tmp/fake-profiler", ProfileServlet.getAsyncProfilerHome());
    } finally {
      if (prev == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, prev);
      }
    }
  }

  // ---- DisabledServlet ----

  @Test
  public void testDisabledServletReturns500() throws Exception {
    ProfileServlet.DisabledServlet disabled = new ProfileServlet.DisabledServlet();
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    disabled.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    assertTrue(body.toString().contains("profiler servlet was disabled"));
  }

  // ---- helpers ----

  private HttpServletRequest mockRequest(Map<String, String[]> paramMap, String... kvPairs) {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getParameterMap()).thenReturn(paramMap);
    for (int i = 0; i < kvPairs.length; i += 2) {
      Mockito.when(req.getParameter(kvPairs[i])).thenReturn(kvPairs[i + 1]);
    }
    return req;
  }

  private ServletConfig mockServletConfig() throws Exception {
    ServletContext ctx = Mockito.mock(ServletContext.class);
    Mockito.when(ctx.getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE))
      .thenReturn(new Configuration(false));
    ServletConfig config = Mockito.mock(ServletConfig.class);
    Mockito.when(config.getServletContext()).thenReturn(ctx);
    return config;
  }
}
