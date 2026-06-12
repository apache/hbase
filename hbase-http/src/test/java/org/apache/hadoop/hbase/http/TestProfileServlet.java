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

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestProfileServlet {

  @BeforeEach
  public void clearLastResultBeforeEach() throws Exception {
    clearLastResult();
  }

  // ---- parseProfileRequest ----

  @Test
  public void testParseProfileRequestDefaults() {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", null,
      "output", null, "event", null, "interval", null, "jstackdepth", null, "bufsize", null,
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
    HttpServletRequest req = mockRequest(flags, "pid", "42", "duration", "60", "output", "tree",
      "event", "alloc", "interval", "1000", "jstackdepth", "256", "bufsize", "100000", "width",
      "1200", "height", "16", "minwidth", "0.5", "refreshDelay", "3");

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
    assertEquals(0.5, parsed.getMinwidth(), 1e-9);
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

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "1",
      "refreshDelay", "2", "output", null, "event", null, "interval", null, "jstackdepth", null,
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

  // ---- doGet error paths ----

  @Test
  public void testDoGetBackendThrowsRuntimeException() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), Mockito.any()))
      .thenThrow(new IllegalStateException("profiler already started"));

    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "1",
      "refreshDelay", null, "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    servlet.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    assertTrue(body.toString().contains("Profiler error"));
  }

  @Test
  public void testDoGetBackendThrowsError() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), Mockito.any()))
      .thenThrow(new UnsatisfiedLinkError("no libasyncProfiler in java.library.path"));

    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "1",
      "refreshDelay", null, "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    servlet.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    assertTrue(body.toString().contains("Profiler error"));
  }

  @Test
  public void testDoGetSecondRequestRejectedWithConflictWhenProfilingActive() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), Mockito.any())).thenReturn("OK");

    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "1",
      "refreshDelay", null, "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);

    // First request succeeds and sets profiling=true.
    HttpServletResponse resp1 = Mockito.mock(HttpServletResponse.class);
    Mockito.when(resp1.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    servlet.doGet(req, resp1);
    Mockito.verify(resp1).setStatus(HttpServletResponse.SC_ACCEPTED);

    // Second request must see profiling=true under the lock and return 409 CONFLICT.
    HttpServletResponse resp2 = Mockito.mock(HttpServletResponse.class);
    StringWriter body2 = new StringWriter();
    Mockito.when(resp2.getWriter()).thenReturn(new PrintWriter(body2));
    servlet.doGet(req, resp2);

    Mockito.verify(resp2).setStatus(HttpServletResponse.SC_CONFLICT);
    assertTrue(body2.toString().contains("already running"));
  }

  // ---- doGet error paths — orphan file cleanup ----

  @Test
  public void testDoGetDeletesOrphanFileWhenExecuteStartThrows() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), fileCaptor.capture()))
      .thenThrow(new IllegalStateException("profiler already started"));

    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "1",
      "refreshDelay", null, "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    servlet.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    // The placeholder created for this specific request must have been deleted.
    File created = fileCaptor.getValue();
    assertFalse(created.exists(),
      "Orphan placeholder must be deleted when executeStart fails: " + created.getName());
  }

  @Test
  public void testStopperThreadWritesPaddedErrorOnExecuteStopFailure() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), Mockito.any())).thenReturn("OK");
    Mockito.when(mockBackend.executeStop(Mockito.any(), Mockito.any()))
      .thenThrow(new IllegalStateException("stop failed"));

    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "1",
      "refreshDelay", null, "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    ArgumentCaptor<String> refreshCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    servlet.doGet(req, resp);
    Mockito.verify(resp).setHeader(Mockito.eq("Refresh"), refreshCaptor.capture());

    // Extract the output file path from the Refresh header URL
    String refreshValue = refreshCaptor.getValue();
    // Refresh header: "1;/prof-output-hbase/<filename>"
    String relUrl = refreshValue.substring(refreshValue.indexOf(';') + 1);
    String fileName = relUrl.substring(relUrl.lastIndexOf('/') + 1);
    File outputFile = new File(ProfileServlet.OUTPUT_DIR, fileName);

    // Wait for the stopper thread to finish (duration=1s + some buffer)
    long deadline = System.currentTimeMillis() + 5000;
    while (outputFile.length() < ProfileServlet.PROF_OUTPUT_MIN_BYTES
      && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }

    byte[] content = Files.readAllBytes(outputFile.toPath());
    assertTrue(content.length > ProfileServlet.PROF_OUTPUT_MIN_BYTES,
      "Stopper must pad error file to > PROF_OUTPUT_MIN_BYTES so ProfileOutputServlet stops polling");
    assertTrue(new String(content, StandardCharsets.UTF_8).contains("stop failed"),
      "Padded error file must contain the failure message");
  }

  @Test
  public void testStopperThreadWritesPaddedErrorOnInterrupt() throws Exception {
    ProfilerBackend mockBackend = Mockito.mock(ProfilerBackend.class);
    Mockito.when(mockBackend.executeStart(Mockito.any(), Mockito.any())).thenReturn("OK");

    // Use a long duration so the stopper is sleeping when we interrupt it
    ProfileServlet servlet = new ProfileServlet(mockBackend);
    servlet.init(mockServletConfig());

    HttpServletRequest req = mockRequest(Collections.emptyMap(), "pid", null, "duration", "60",
      "refreshDelay", null, "output", null, "event", null, "interval", null, "jstackdepth", null,
      "bufsize", null, "width", null, "height", null, "minwidth", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    ArgumentCaptor<String> refreshCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    servlet.doGet(req, resp);
    Mockito.verify(resp).setHeader(Mockito.eq("Refresh"), refreshCaptor.capture());

    String relUrl = refreshCaptor.getValue().substring(refreshCaptor.getValue().indexOf(';') + 1);
    String fileName = relUrl.substring(relUrl.lastIndexOf('/') + 1);
    File outputFile = new File(ProfileServlet.OUTPUT_DIR, fileName);

    // Find the stopper thread by name and interrupt it while it is sleeping
    Thread stopper = null;
    long findDeadline = System.currentTimeMillis() + 2000;
    while (stopper == null && System.currentTimeMillis() < findDeadline) {
      for (Thread t : Thread.getAllStackTraces().keySet()) {
        if ("ProfileServlet-stopper".equals(t.getName()) && t.isAlive()) {
          stopper = t;
          break;
        }
      }
      if (stopper == null) {
        Thread.sleep(10);
      }
    }
    assertNotNull(stopper, "ProfileServlet-stopper thread must be alive during the sleep");
    stopper.interrupt();

    // Wait for the stopper to write the interrupted-session message
    long deadline = System.currentTimeMillis() + 3000;
    while (outputFile.length() < ProfileServlet.PROF_OUTPUT_MIN_BYTES
      && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }

    byte[] content = Files.readAllBytes(outputFile.toPath());
    assertTrue(content.length > ProfileServlet.PROF_OUTPUT_MIN_BYTES,
      "Stopper must pad interrupted-session file to > PROF_OUTPUT_MIN_BYTES");
    assertTrue(new String(content, StandardCharsets.UTF_8).contains("interrupted"),
      "Padded error file must contain 'interrupted'");
  }

  // ---- ?last ----

  @Test
  public void testLastReturns404WhenNoResultCached() throws Exception {
    clearLastResult();

    ProfileServlet servlet = new ProfileServlet(null);
    servlet.init(mockServletConfig());
    Map<String, String[]> params = new HashMap<>();
    params.put("last", new String[] { "" });
    HttpServletRequest req = mockRequest(params, "pid", null, "duration", null, "output", null,
      "event", null, "interval", null, "jstackdepth", null, "bufsize", null, "width", null,
      "height", null, "minwidth", null, "refreshDelay", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    servlet.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_NOT_FOUND);
    assertTrue(body.toString().contains("No profiling results available yet"));
  }

  @Test
  public void testLastRedirectsToMostRecentResult() throws Exception {
    String expectedUrl = "/prof-output-hbase/profile-cpu-20260612-120000.html";
    setLastResult(new ProfileServlet.ProfileResult(expectedUrl, "cpu", 10, Instant.now()));

    ProfileServlet servlet = new ProfileServlet(null);
    servlet.init(mockServletConfig());
    Map<String, String[]> params = new HashMap<>();
    params.put("last", new String[] { "" });
    HttpServletRequest req = mockRequest(params, "pid", null, "duration", null, "output", null,
      "event", null, "interval", null, "jstackdepth", null, "bufsize", null, "width", null,
      "height", null, "minwidth", null, "refreshDelay", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    servlet.doGet(req, resp);

    Mockito.verify(resp).sendRedirect(expectedUrl);
  }

  @Test
  public void testLastResultOverwrittenByNewSession() throws Exception {
    String firstUrl = "/prof-output-hbase/profile-cpu-first.html";
    String secondUrl = "/prof-output-hbase/profile-cpu-second.html";
    setLastResult(new ProfileServlet.ProfileResult(firstUrl, "cpu", 10, Instant.now()));

    // Overwrite with a second result — only the latest is kept
    setLastResult(new ProfileServlet.ProfileResult(secondUrl, "cpu", 30, Instant.now()));

    ProfileServlet servlet = new ProfileServlet(null);
    servlet.init(mockServletConfig());
    Map<String, String[]> params = new HashMap<>();
    params.put("last", new String[] { "" });
    HttpServletRequest req = mockRequest(params, "pid", null, "duration", null, "output", null,
      "event", null, "interval", null, "jstackdepth", null, "bufsize", null, "width", null,
      "height", null, "minwidth", null, "refreshDelay", null);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));

    servlet.doGet(req, resp);

    // Only the most recent result is cached — redirect must point to secondUrl, not firstUrl
    Mockito.verify(resp).sendRedirect(secondUrl);
    Mockito.verify(resp, Mockito.never()).sendRedirect(firstUrl);
  }

  // ---- parseProfileRequest — enum fallbacks ----

  @Test
  public void testGetOutputFallsBackToHtmlOnUnknownValue() {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(Collections.emptyMap(), "output", "bogusformat", "pid",
      null, "duration", null, "event", null, "interval", null, "jstackdepth", null, "bufsize", null,
      "width", null, "height", null, "minwidth", null, "refreshDelay", null);
    assertEquals(ProfileServlet.Output.HTML, servlet.parseProfileRequest(req).getOutput());
  }

  @Test
  public void testGetEventFallsBackToCpuOnUnknownValue() {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(Collections.emptyMap(), "event", "bogusevent", "pid", null,
      "duration", null, "output", null, "interval", null, "jstackdepth", null, "bufsize", null,
      "width", null, "height", null, "minwidth", null, "refreshDelay", null);
    assertEquals(ProfileServlet.Event.CPU, servlet.parseProfileRequest(req).getEvent());
  }

  @Test
  public void testDurationClampedToMinOne() {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(Collections.emptyMap(), "duration", "0", "pid", null,
      "output", null, "event", null, "interval", null, "jstackdepth", null, "bufsize", null,
      "width", null, "height", null, "minwidth", null, "refreshDelay", null);
    assertEquals(1, servlet.parseProfileRequest(req).getDuration());

    HttpServletRequest negReq = mockRequest(Collections.emptyMap(), "duration", "-5", "pid", null,
      "output", null, "event", null, "interval", null, "jstackdepth", null, "bufsize", null,
      "width", null, "height", null, "minwidth", null, "refreshDelay", null);
    assertEquals(1, servlet.parseProfileRequest(negReq).getDuration());
  }

  @Test
  public void testDurationCappedAtMax() {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = mockRequest(Collections.emptyMap(), "duration", "999999", "pid", null,
      "output", null, "event", null, "interval", null, "jstackdepth", null, "bufsize", null,
      "width", null, "height", null, "minwidth", null, "refreshDelay", null);
    assertEquals(ProfileServlet.MAX_DURATION_SECONDS,
      servlet.parseProfileRequest(req).getDuration());
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
  public void testDisabledServletReturns500WithDefaultReason() throws Exception {
    ProfileServlet.DisabledServlet disabled = new ProfileServlet.DisabledServlet();
    disabled.init(mockServletConfig());
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    disabled.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    // No reason param set — falls back to the default message
    assertTrue(body.toString().contains("disabled at startup"));
  }

  @Test
  public void testDisabledServletReturns500WithCustomReason() throws Exception {
    ProfileServlet.DisabledServlet disabled = new ProfileServlet.DisabledServlet();
    ServletConfig config = Mockito.mock(ServletConfig.class);
    ServletContext ctx = Mockito.mock(ServletContext.class);
    Mockito.when(config.getServletContext()).thenReturn(ctx);
    Mockito.when(config.getInitParameter(ProfileServlet.DisabledServlet.REASON_PARAM))
      .thenReturn("disabled via hbase.profiler.enabled=false");
    disabled.init(config);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    Mockito.when(resp.getWriter()).thenReturn(new PrintWriter(body));

    disabled.doGet(req, resp);

    Mockito.verify(resp).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    assertTrue(body.toString().contains("hbase.profiler.enabled=false"));
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

  private static Field lastResultField() throws Exception {
    Field f = ProfileServlet.class.getDeclaredField("lastResult");
    f.setAccessible(true);
    return f;
  }

  private static void setLastResult(ProfileServlet.ProfileResult result) throws Exception {
    lastResultField().set(null, result);
  }

  private static void clearLastResult() throws Exception {
    lastResultField().set(null, null);
  }
}
