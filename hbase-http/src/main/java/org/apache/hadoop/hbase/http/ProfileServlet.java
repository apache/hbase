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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import one.profiler.AsyncProfiler;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

/**
 * Servlet that runs async-profiler as web-endpoint. Following options from async-profiler can be
 * specified as query paramater. // -e event profiling event: cpu|alloc|lock|cache-misses etc. // -d
 * duration run profiling for 'duration' seconds (integer) // -i interval sampling interval in
 * nanoseconds (long) // -j jstackdepth maximum Java stack depth (integer) // -b bufsize frame
 * buffer size (long) // -t profile different threads separately // -s simple class names instead of
 * FQN // -o fmt[,fmt...] output format: summary|traces|flat|collapsed|svg|tree|jfr|html // --width
 * px SVG width pixels (integer) // --height px SVG frame height pixels (integer) // --minwidth px
 * skip frames smaller than px (double) // --reverse generate stack-reversed FlameGraph / Call tree
 * Example: - To collect 30 second CPU profile of current process (returns FlameGraph svg) curl
 * "http://localhost:10002/prof" - To collect 1 minute CPU profile of current process and output in
 * tree format (html) curl "http://localhost:10002/prof?output=tree&amp;duration=60" - To collect 30
 * second heap allocation profile of current process (returns FlameGraph svg) curl
 * "http://localhost:10002/prof?event=alloc" - To collect lock contention profile of current process
 * (returns FlameGraph svg) curl "http://localhost:10002/prof?event=lock" Following event types are
 * supported (default is 'cpu') (NOTE: not all OS'es support all events) // Perf events: // cpu //
 * page-faults // context-switches // cycles // instructions // cache-references // cache-misses //
 * branches // branch-misses // bus-cycles // L1-dcache-load-misses // LLC-load-misses //
 * dTLB-load-misses // mem:breakpoint // trace:tracepoint // Java events: // alloc // lock
 */
@InterfaceAudience.Private
public class ProfileServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProfileServlet.class);

  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ALLOWED_METHODS = "GET";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
  private static final int DEFAULT_DURATION_SECONDS = 10;
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);
  static final String OUTPUT_DIR = System.getProperty("java.io.tmpdir") + "/prof-output-hbase";

  enum Event {
    CPU("cpu"),
    WALL("wall"),
    ALLOC("alloc"),
    LOCK("lock"),
    PAGE_FAULTS("page-faults"),
    CONTEXT_SWITCHES("context-switches"),
    CYCLES("cycles"),
    INSTRUCTIONS("instructions"),
    CACHE_REFERENCES("cache-references"),
    CACHE_MISSES("cache-misses"),
    BRANCHES("branches"),
    BRANCH_MISSES("branch-misses"),
    BUS_CYCLES("bus-cycles"),
    L1_DCACHE_LOAD_MISSES("L1-dcache-load-misses"),
    LLC_LOAD_MISSES("LLC-load-misses"),
    DTLB_LOAD_MISSES("dTLB-load-misses"),
    MEM_BREAKPOINT("mem:breakpoint"),
    TRACE_TRACEPOINT("trace:tracepoint"),;

    private final String internalName;

    Event(final String internalName) {
      this.internalName = internalName;
    }

    public String getInternalName() {
      return internalName;
    }

    public static Event fromInternalName(final String name) {
      for (Event event : values()) {
        if (event.getInternalName().equalsIgnoreCase(name)) {
          return event;
        }
      }

      return null;
    }
  }

  enum Output {
    SUMMARY,
    TRACES,
    FLAT,
    COLLAPSED,
    // No SVG in 2.x asyncprofiler.
    SVG,
    TREE,
    JFR,
    // In 2.x asyncprofiler, this is how you get flamegraphs.
    HTML
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED",
      justification = "This class is never serialized nor restored.")
  private transient Lock profilerLock = new ReentrantLock();
  private transient volatile boolean profiling;
  private final long currentPid = ProcessHandle.current().pid();

  public static final class ProfileRequest {
    private final int duration;
    private final Output output;
    private final Event event;
    private final Long interval;
    private final Integer jstackDepth;
    private final Long bufsize;
    private final boolean thread;
    private final boolean simple;
    private final Integer width;
    private final Integer height;
    private final Double minwidth;
    private final boolean reverse;
    private final int refreshDelay;
    private final Integer pid;

    private ProfileRequest(int duration, Output output, Event event, Long interval,
      Integer jstackDepth, Long bufsize, boolean thread, boolean simple, Integer width,
      Integer height, Double minwidth, boolean reverse, int refreshDelay, Integer pid) {
      this.duration = duration;
      this.output = output;
      this.event = event;
      this.interval = interval;
      this.jstackDepth = jstackDepth;
      this.bufsize = bufsize;
      this.thread = thread;
      this.simple = simple;
      this.width = width;
      this.height = height;
      this.minwidth = minwidth;
      this.reverse = reverse;
      this.refreshDelay = refreshDelay;
      this.pid = pid;
    }

    public int getDuration() {
      return duration;
    }

    public Output getOutput() {
      return output;
    }

    public Event getEvent() {
      return event;
    }

    public Long getInterval() {
      return interval;
    }

    public Integer getJstackDepth() {
      return jstackDepth;
    }

    public Long getBufsize() {
      return bufsize;
    }

    public boolean isThread() {
      return thread;
    }

    public boolean isSimple() {
      return simple;
    }

    public Integer getWidth() {
      return width;
    }

    public Integer getHeight() {
      return height;
    }

    public Double getMinwidth() {
      return minwidth;
    }

    public boolean isReverse() {
      return reverse;
    }

    public int getRefreshDelay() {
      return refreshDelay;
    }

    public Integer getPid() {
      return pid;
    }
  }

  public ProfileServlet() {
    LOG.info("ProfileServlet initialized");
  }

  public ProfileRequest parseProfileRequest(final HttpServletRequest req) {
    // Note: when using in-process async-profiler Java API, we can only profile this JVM.
    // We keep the pid parameter for API compatibility, but do not support external processes.
    Integer requestedPid = getInteger(req, "pid", null);

    final int duration = getInteger(req, "duration", DEFAULT_DURATION_SECONDS);
    final Output output = getOutput(req);
    final Event event = getEvent(req);
    final Long interval = getLong(req, "interval");
    final Integer jstackDepth = getInteger(req, "jstackdepth", null);
    final Long bufsize = getLong(req, "bufsize");
    final boolean thread = req.getParameterMap().containsKey("thread");
    final boolean simple = req.getParameterMap().containsKey("simple");
    final Integer width = getInteger(req, "width", null);
    final Integer height = getInteger(req, "height", null);
    final Double minwidth = getMinWidth(req);
    final boolean reverse = req.getParameterMap().containsKey("reverse");
    int refreshDelay = getInteger(req, "refreshDelay", 0);

    return new ProfileRequest(duration, output, event, interval, jstackDepth, bufsize, thread,
      simple, width, height, minwidth, reverse, refreshDelay, requestedPid);
  }

  public String buildStartCommand(final ProfileRequest request) {
    StringBuilder sb = new StringBuilder("start");
    sb.append(",event=").append(request.getEvent().getInternalName());
    appendOption(sb, "interval", request.getInterval());
    appendOption(sb, "jstackdepth", request.getJstackDepth());
    appendOption(sb, "bufsize", request.getBufsize());
    if (request.isThread()) {
      sb.append(",threads");
    }
    if (request.isSimple()) {
      sb.append(",simple");
    }
    return sb.toString();
  }

  public String buildStopCommand(final ProfileRequest request, final File outputFile) {
    StringBuilder sb = new StringBuilder("stop");
    sb.append(",file=").append(outputFile.getAbsolutePath());
    sb.append(",format=").append(mapOutputToAsyncProfilerFormat(request.getOutput()));
    appendOption(sb, "width", request.getWidth());
    appendOption(sb, "height", request.getHeight());
    appendOption(sb, "minwidth", request.getMinwidth());
    if (request.isReverse()) {
      sb.append(",reverse");
    }
    return sb.toString();
  }

  private void appendOption(final StringBuilder sb, final String key, final Object value) {
    if (value != null) {
      sb.append(',').append(key).append('=').append(value);
    }
  }

  protected String executeProfiler(String command) throws IOException {
    return AsyncProfiler.getInstance().execute(command);
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
    throws IOException {
    if (!checkInstrumentationAccess(req, resp)) {
      return;
    }

    final ProfileRequest request = parseProfileRequest(req);

    // We keep the pid parameter for backward compatibility but only support profiling this JVM.
    if (request.getPid() != null && request.getPid().longValue() != currentPid) {
      writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        "The 'pid' parameter is only supported for the current process when using the "
          + "embedded async-profiler library.");
      return;
    }

    if (profiling) {
      writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        "Another instance of profiler is already running.");
      return;
    }

    int lockTimeoutSecs = 3;
    boolean locked = false;
    try {
      locked = profilerLock.tryLock(lockTimeoutSecs, TimeUnit.SECONDS);
      if (!locked) {
        writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Unable to acquire lock. Another instance of profiler might be running.");
        LOG.warn("Unable to acquire lock in " + lockTimeoutSecs
          + " seconds. Another instance of profiler might be running.");
        return;
      }

      File outputFile = createOutputFile(request);
      // Ensure the file exists so ProfileOutputServlet can poll until it is complete.
      Files.write(outputFile.toPath(), new byte[0]);

      String startCmd = buildStartCommand(request);
      executeProfiler(startCmd);
      profiling = true;

      String stopCmd = buildStopCommand(request, outputFile);
      startStopperThread(request.getDuration(), stopCmd, outputFile);

      List<String> visible = new ArrayList<>(2);
      visible.add(startCmd);
      visible.add(stopCmd);
      writeAcceptedResponse(resp, request, outputFile, visible);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while acquiring profile lock.", e);
      writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        "Interrupted while acquiring profile lock.");
    } finally {
      if (locked) {
        profilerLock.unlock();
      }
    }
  }

  private void startStopperThread(final int durationSeconds, final String stopCmd,
    final File outputFile) {
    Thread t = new Thread(() -> {
      try {
        TimeUnit.SECONDS.sleep(durationSeconds);
        executeProfiler(stopCmd);
      } catch (Exception e) {
        try {
          Files.write(outputFile.toPath(),
            ("Profiler failed: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        } catch (IOException ioe) {
          LOG.warn("Unable to write profiler error to output file", ioe);
        }
        LOG.warn("Profiler stop/dump failed", e);
      } finally {
        profiling = false;
      }
    }, "ProfileServlet-stopper");
    t.setDaemon(true);
    t.start();
  }

  private boolean checkInstrumentationAccess(final HttpServletRequest req,
    final HttpServletResponse resp) throws IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), req, resp)) {
      resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      setResponseHeader(resp);
      resp.getWriter().write("Unauthorized: Instrumentation access is not allowed!");
      return false;
    }
    return true;
  }

  private String mapOutputToAsyncProfilerFormat(Output output) {
    switch (output) {
      case SUMMARY:
        return "summary";
      case TRACES:
        return "traces";
      case FLAT:
        return "flat";
      case COLLAPSED:
        return "collapsed";
      case TREE:
        return "tree";
      case JFR:
        return "jfr";
      case SVG:
        return "svg";
      case HTML:
      default:
        return "html";
    }
  }

  private void writeError(final HttpServletResponse resp, final int status, final String message)
    throws IOException {
    resp.setStatus(status);
    setResponseHeader(resp);
    resp.getWriter().write(message);
  }

  private File createOutputFile(final ProfileRequest request) throws IOException {
    final long pid = request.getPid() != null ? request.getPid().longValue() : currentPid;
    File outputFile =
      new File(OUTPUT_DIR, "async-prof-pid-" + pid + "-" + request.getEvent().name().toLowerCase()
        + "-" + ID_GEN.incrementAndGet() + "." + request.getOutput().name().toLowerCase());
    Files.createDirectories(Paths.get(OUTPUT_DIR));
    return outputFile;
  }

  private void writeAcceptedResponse(final HttpServletResponse resp, final ProfileRequest request,
    final File outputFile, final List<String> cmd) throws IOException {
    setResponseHeader(resp);
    resp.setStatus(HttpServletResponse.SC_ACCEPTED);
    String relativeUrl = "/prof-output-hbase/" + outputFile.getName();
    resp.getWriter()
      .write("Started [" + request.getEvent().getInternalName()
        + "] profiling. This page will automatically redirect to " + relativeUrl + " after "
        + request.getDuration() + " seconds. "
        + "If empty diagram and Linux 4.6+, see 'Basic Usage' section on the Async "
        + "Profiler Home Page, https://github.com/jvm-profiling-tools/async-profiler."
        + "\n\nCommand:\n" + Joiner.on(" ").join(cmd));

    resp.setHeader("Refresh",
      (request.getDuration() + request.getRefreshDelay()) + ";" + relativeUrl);
    resp.getWriter().flush();
  }

  private Integer getInteger(final HttpServletRequest req, final String param,
    final Integer defaultValue) {
    final String value = req.getParameter(param);
    if (value != null) {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private Long getLong(final HttpServletRequest req, final String param) {
    final String value = req.getParameter(param);
    if (value != null) {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Double getMinWidth(final HttpServletRequest req) {
    final String value = req.getParameter("minwidth");
    if (value != null) {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Event getEvent(final HttpServletRequest req) {
    final String eventArg = req.getParameter("event");
    if (eventArg != null) {
      Event event = Event.fromInternalName(eventArg);
      return event == null ? Event.CPU : event;
    }
    return Event.CPU;
  }

  private Output getOutput(final HttpServletRequest req) {
    final String outputArg = req.getParameter("output");
    if (req.getParameter("output") != null) {
      try {
        return Output.valueOf(outputArg.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        return Output.HTML;
      }
    }
    return Output.HTML;
  }

  static void setResponseHeader(final HttpServletResponse response) {
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    response.setContentType(CONTENT_TYPE_TEXT);
  }

  public static class DisabledServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter()
        .write("The profiler servlet was disabled at startup.\n\n"
          + "Please ensure the prerequisites for the Profiler Servlet have been installed and the\n"
          + "environment is properly configured. For more information please see\n"
          + "http://hbase.apache.org/book.html#profiler\n");
      return;
    }

  }

}
