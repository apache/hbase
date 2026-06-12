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
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet that runs async-profiler as a web endpoint.
 * <p>
 * Query parameters:
 * <ul>
 * <li>{@code event} - profiling event: cpu|alloc|lock|cache-misses etc. (default: cpu)</li>
 * <li>{@code duration} - run profiling for N seconds, clamped to [1,
 * {@value #MAX_DURATION_SECONDS}] (default: 10)</li>
 * <li>{@code interval} - sampling interval in nanoseconds (long)</li>
 * <li>{@code jstackdepth} - maximum Java stack depth (integer)</li>
 * <li>{@code bufsize} - frame buffer size (long); honored only by BinaryBackend</li>
 * <li>{@code thread} - profile different threads separately (flag)</li>
 * <li>{@code simple} - simple class names instead of FQN (flag)</li>
 * <li>{@code output} - output format: summary|traces|flat|collapsed|tree|jfr|html (default:
 * html)</li>
 * <li>{@code width} - flame graph width in pixels; honored only by BinaryBackend</li>
 * <li>{@code height} - flame graph frame height in pixels; honored only by BinaryBackend</li>
 * <li>{@code minwidth} - skip frames smaller than this width in pixels (double)</li>
 * <li>{@code reverse} - generate stack-reversed FlameGraph / Call tree (flag)</li>
 * <li>{@code pid} - target process ID; LibraryBackend only supports the current JVM (returns 400
 * for other PIDs), BinaryBackend supports external PIDs</li>
 * <li>{@code refreshDelay} - extra seconds added to the auto-refresh delay (integer)</li>
 * <li>{@code last} - instead of starting a new session, redirect to the most recently completed
 * profiling result. Returns 404 if no result is cached yet. The last result is kept in memory for
 * the lifetime of the JVM.</li>
 * </ul>
 * <p>
 * Examples:
 *
 * <pre>
 * # 30-second CPU profile (default)
 * curl "http://localhost:10002/prof"
 *
 * # 1-minute allocation profile in tree format
 * curl "http://localhost:10002/prof?event=alloc&amp;output=tree&amp;duration=60"
 *
 * # Redirect to the most recent profiling result
 * curl "http://localhost:10002/prof?last"
 * </pre>
 * <p>
 * Profiling is single-flight: only one session runs at a time. A second request while a session is
 * active returns HTTP 409 Conflict with the URL of the last completed result (if any). Closing the
 * browser tab does not cancel a running session — the stopper thread runs to completion on the
 * server.
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
  static final int MAX_DURATION_SECONDS = 3600;
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);
  static final String OUTPUT_DIR = System.getProperty("java.io.tmpdir") + "/prof-output-hbase";
  // ProfileOutputServlet considers a file complete when its size exceeds this threshold.
  // Error messages written to the output file must be padded past this limit.
  static final int PROF_OUTPUT_MIN_BYTES = 100;

  private static final String ASYNC_PROFILER_HOME_ENV = "ASYNC_PROFILER_HOME";
  private static final String ASYNC_PROFILER_HOME_SYSTEM_PROPERTY = "async.profiler.home";

  /** Immutable record of a completed profiling session. */
  static final class ProfileResult {
    final String relativeUrl;
    final String event;
    final int durationSeconds;
    final Instant completedAt;

    ProfileResult(String relativeUrl, String event, int durationSeconds, Instant completedAt) {
      this.relativeUrl = relativeUrl;
      this.event = event;
      this.durationSeconds = durationSeconds;
      this.completedAt = completedAt;
    }
  }

  // Last completed profiling result — static so it survives servlet reloads within the same JVM.
  private static volatile ProfileResult lastResult = null;

  // Cached backend detection result — computed once at class-load time so that isAvailable()
  // and the default constructor do not each pay the reflective detection cost.
  private static final ProfilerBackend DETECTED_BACKEND =
    ProfilerBackend.detect(getAsyncProfilerHome());

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
    // SVG dropped in async-profiler 2.0 (HBASE-25685); remapped to HTML by ProfilerCommandMapper.
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
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SE_BAD_FIELD",
      justification = "This class is never serialized nor restored.")
  private final ProfilerBackend backend;

  @InterfaceAudience.Private
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
    this.backend = DETECTED_BACKEND;
    LOG.info("ProfileServlet initialized with backend: {}",
      backend != null ? backend.getClass().getSimpleName() : "none");
  }

  // visible for testing
  ProfileServlet(ProfilerBackend backend) {
    this.backend = backend;
  }

  @Override
  public void init() throws javax.servlet.ServletException {
    super.init();
    try {
      Files.createDirectories(Paths.get(OUTPUT_DIR));
    } catch (IOException e) {
      throw new javax.servlet.ServletException(
        "Failed to create profiler output directory: " + OUTPUT_DIR, e);
    }
  }

  static String getAsyncProfilerHome() {
    String home = System.getenv(ASYNC_PROFILER_HOME_ENV);
    if (home == null || home.trim().isEmpty()) {
      home = System.getProperty(ASYNC_PROFILER_HOME_SYSTEM_PROPERTY);
    }
    return home;
  }

  /**
   * Returns true if a profiler backend was detected at class-load time. Detection is a one-shot
   * operation: a library added to the classpath after the JVM starts requires a restart. A backend
   * that resolved successfully here may still fail on first use if the native binary is
   * incompatible with the OS/kernel — that error surfaces at request time via the
   * {@code catch(Error | RuntimeException)} block in {@link #doGet}.
   */
  public static boolean isAvailable() {
    return DETECTED_BACKEND != null;
  }

  public ProfileRequest parseProfileRequest(final HttpServletRequest req) {
    // Note: when using in-process async-profiler Java API, we can only profile this JVM.
    // We keep the pid parameter for API compatibility, but do not support external processes.
    Integer requestedPid = getInteger(req, "pid", null);

    final int duration = Math.min(
      Math.max(getInteger(req, "duration", DEFAULT_DURATION_SECONDS), 1), MAX_DURATION_SECONDS);
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

  protected String executeStart(ProfileRequest request, File outputFile) throws IOException {
    return backend.executeStart(request, outputFile);
  }

  protected String executeStop(ProfileRequest request, File outputFile) throws IOException {
    return backend.executeStop(request, outputFile);
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
    throws IOException {
    if (!checkInstrumentationAccess(req, resp)) {
      return;
    }

    // ?last — redirect to the most recent completed profiling result, or list recent results.
    if (req.getParameterMap().containsKey("last")) {
      ProfileResult last = lastResult;
      if (last == null) {
        writeError(resp, HttpServletResponse.SC_NOT_FOUND,
          "No profiling results available yet. Run /prof to start a session.");
      } else {
        setResponseHeader(resp);
        resp.sendRedirect(last.relativeUrl);
      }
      return;
    }

    final ProfileRequest request = parseProfileRequest(req);

    // LibraryBackend can only profile the current JVM; BinaryBackend supports external PIDs.
    if (
      request.getPid() != null && request.getPid().longValue() != currentPid
        && backend instanceof LibraryBackend
    ) {
      LOG.warn("Rejected profiling request for PID {} (current PID: {}) — "
        + "LibraryBackend only supports the current process", request.getPid(), currentPid);
      writeError(resp, HttpServletResponse.SC_BAD_REQUEST,
        "The 'pid' parameter is only supported for the current process when using the "
          + "LibraryBackend (in-process async-profiler). Use ASYNC_PROFILER_HOME to enable "
          + "the BinaryBackend for cross-process profiling.");
      return;
    }

    int lockTimeoutSecs = 3;
    boolean locked = false;
    boolean thisRequestSetProfiling = false;
    boolean stopperStarted = false;
    File outputFile = null;
    try {
      locked = profilerLock.tryLock(lockTimeoutSecs, TimeUnit.SECONDS);
      if (!locked) {
        writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Unable to acquire lock. Another instance of profiler might be running.");
        LOG.warn("Unable to acquire lock in " + lockTimeoutSecs
          + " seconds. Another instance of profiler might be running.");
        return;
      }

      // Re-check under the lock to close the TOCTOU window.
      if (profiling) {
        StringBuilder msg = new StringBuilder("Another instance of profiler is already running.");
        ProfileResult last = lastResult;
        if (last != null) {
          msg.append(" Last result: ").append(last.relativeUrl).append(" (").append(last.event)
            .append(", ").append(last.durationSeconds).append("s, completed ")
            .append(last.completedAt).append("). Use /prof?last to view it.");
        }
        writeError(resp, HttpServletResponse.SC_CONFLICT, msg.toString());
        return;
      }

      outputFile = createOutputFile(request);
      final String relativeUrl = "/prof-output-hbase/" + outputFile.getName();
      // Write the placeholder before starting the profiler. If executeStart succeeds but a
      // subsequent step throws, the stopper thread (which owns profiling=false) will write the
      // error message to this file. Without the file, a partial-start failure would leave the
      // profiler running with no stopper and profiling=false, allowing a second concurrent session.
      Files.write(outputFile.toPath(), new byte[0]);
      executeStart(request, outputFile);
      profiling = true;
      thisRequestSetProfiling = true;

      startStopperThread(request.getDuration(), request, outputFile, relativeUrl);
      stopperStarted = true;

      writeAcceptedResponse(resp, request, relativeUrl);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while acquiring profile lock.", e);
      writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        "Interrupted while acquiring profile lock.");
    } catch (IOException | Error | RuntimeException e) {
      // Catches:
      // - IOException: AsyncProfiler.execute() throws IOException for invalid agent commands
      // - UnsatisfiedLinkError / other Error: native lib absent or incompatible OS/kernel
      // - IllegalStateException / IllegalArgumentException (RuntimeException): double-start,
      // unsupported event, rejected format from the profiler API
      LOG.warn("Profiler failed to start or execute", e);
      writeError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        "Profiler error: " + e.getMessage()
          + ". Check that the async-profiler native library is compatible with this OS/kernel.");
      // Delete the placeholder whenever the stopper thread was never started — in that case
      // no client received the output URL and the stopper will never write a result to the file.
      // Guard on !stopperStarted (not !thisRequestSetProfiling) to also cover the rare path where
      // executeStart succeeded but t.start() threw, leaving a 0-byte file with no owner.
      if (!stopperStarted && outputFile != null) {
        try {
          Files.deleteIfExists(outputFile.toPath());
        } catch (IOException ioe) {
          LOG.warn("Unable to delete orphan placeholder {}", outputFile.getName(), ioe);
        }
      }
    } finally {
      // Only reset the profiling flag if THIS request was the one that set it, and the stopper
      // thread was never started (e.g. t.start() threw OutOfMemoryError). Using a separate flag
      // avoids incorrectly clearing profiling=true for a concurrently-running session when this
      // request exited early via the 409 conflict path.
      if (thisRequestSetProfiling && !stopperStarted) {
        profiling = false;
      }
      if (locked) {
        profilerLock.unlock();
      }
    }
  }

  private void startStopperThread(final int durationSeconds, final ProfileRequest request,
    final File outputFile, final String relativeUrl) {
    Thread t = new Thread(() -> {
      boolean succeeded = false;
      Throwable failure = null;
      try {
        TimeUnit.SECONDS.sleep(durationSeconds);
        executeStop(request, outputFile);
        lastResult = new ProfileResult(relativeUrl, request.getEvent().getInternalName(),
          durationSeconds, Instant.now());
        succeeded = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        failure = e;
        LOG.warn("Profiler stopper thread interrupted; profiling session may not have stopped"
          + " cleanly.", e);
      } catch (Throwable e) {
        failure = e;
        LOG.warn("Profiler stop/dump failed", e);
      } finally {
        // If the session did not complete successfully, pad the output file to >100 bytes so
        // ProfileOutputServlet's size check treats it as done and stops auto-refreshing.
        if (!succeeded && failure != null) {
          try {
            String msg = (failure instanceof InterruptedException)
              ? "Profiler session interrupted before stop completed."
              : "Profiler stop/dump failed: " + failure.getMessage();
            // PROF_OUTPUT_MIN_BYTES is checked by ProfileOutputServlet to determine completion.
            while (msg.length() <= PROF_OUTPUT_MIN_BYTES) {
              msg += " ";
            }
            Files.write(outputFile.toPath(), msg.getBytes(StandardCharsets.UTF_8));
          } catch (IOException ioe) {
            LOG.warn("Unable to write profiler error to output file", ioe);
          }
        }
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

  @Override
  public void destroy() {
    if (backend != null) {
      backend.destroy();
    }
    super.destroy();
  }

  private void writeError(final HttpServletResponse resp, final int status, final String message)
    throws IOException {
    resp.setStatus(status);
    setResponseHeader(resp);
    resp.getWriter().write(message);
  }

  private File createOutputFile(final ProfileRequest request) throws IOException {
    final long pid = request.getPid() != null ? request.getPid().longValue() : currentPid;
    // Use the remapped file extension so that (e.g.) SVG→HTML remap is reflected in the
    // filename. toFileExtension is used here (not toFormatString) to avoid a duplicate
    // LOG.warn — the warning is emitted once by toLibraryStopCommand or toCliCommand.
    String ext = ProfilerCommandMapper.toFileExtension(request.getOutput());
    File outputFile = new File(OUTPUT_DIR, "async-prof-pid-" + pid + "-"
      + request.getEvent().name().toLowerCase() + "-" + ID_GEN.incrementAndGet() + "." + ext);
    return outputFile;
  }

  private void writeAcceptedResponse(final HttpServletResponse resp, final ProfileRequest request,
    final String relativeUrl) throws IOException {
    setResponseHeader(resp);
    resp.setStatus(HttpServletResponse.SC_ACCEPTED);
    StringBuilder body = new StringBuilder();
    body.append("Started [").append(request.getEvent().getInternalName())
      .append("] profiling. This page will automatically redirect to ").append(relativeUrl)
      .append(" after ").append(request.getDuration()).append(" seconds. ")
      .append("If empty diagram and Linux 4.6+, see 'Basic Usage' section on the Async ")
      .append("Profiler Home Page, https://github.com/jvm-profiling-tools/async-profiler.");
    if (backend instanceof LibraryBackend) {
      if (request.getBufsize() != null) {
        body.append("\nNote: bufsize= is not supported by the in-process LibraryBackend"
          + " (async-profiler 4.x) and was ignored."
          + " Set ASYNC_PROFILER_HOME to use BinaryBackend if you need -b support.");
      }
      if (request.getWidth() != null || request.getHeight() != null) {
        body.append("\nNote: width= and height= are not supported by the in-process LibraryBackend"
          + " (async-profiler 4.x) and were ignored."
          + " Set ASYNC_PROFILER_HOME to use BinaryBackend if you need --width/--height support.");
      }
    }
    resp.getWriter().write(body.toString());
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

    /** Init-param key for the human-readable disable reason. */
    static final String REASON_PARAM = "disabledReason";

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      String reason = getInitParameter(REASON_PARAM);
      if (reason == null || reason.isEmpty()) {
        reason = "The profiler servlet was disabled at startup.";
      }
      resp.getWriter().write(reason + "\n\nFor more information please see "
        + "https://hbase.apache.org/docs/profiler\n");
    }

  }

}
