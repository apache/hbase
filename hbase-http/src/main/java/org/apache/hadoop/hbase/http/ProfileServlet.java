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
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.hadoop.hbase.util.ProcessUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

/**
 * Servlet that runs async-profiler as web-endpoint. Following options from async-profiler can be
 * specified as query parameter.
 * <ul>
 * <li>-e event profiling event: cpu|alloc|lock|cache-misses etc.</li>
 * <li>-d duration run profiling for 'duration' seconds (integer), default 10s</li>
 * <li>-i interval sampling interval in nanoseconds (long), default 10ms</li>
 * <li>-j jstackdepth maximum Java stack depth (integer), default 2048</li>
 * <li>-t profile different threads separately</li>
 * <li>-s simple class names instead of FQN</li>
 * <li>-g print method signatures</li>
 * <li>-a annotate Java methods</li>
 * <li>-l prepend library names</li>
 * <li>-o fmt output format: flat|traces|collapsed|flamegraph|tree|jfr</li>
 * <li>--minwidth pct skip frames smaller than pct% (double)</li>
 * <li>--reverse generate stack-reversed FlameGraph / Call tree</li>
 * </ul>
 * Example:
 * <ul>
 * <li>To collect 30 second CPU profile of current process (returns FlameGraph svg):
 * {@code curl http://localhost:10002/prof"}</li>
 * <li>To collect 1 minute CPU profile of current process and output in tree format (html)
 * {@code curl "http://localhost:10002/prof?output=tree&amp;duration=60"}</li>
 * <li>To collect 30 second heap allocation profile of current process (returns FlameGraph):
 * {@code curl "http://localhost:10002/prof?event=alloc"}</li>
 * <li>To collect lock contention profile of current process (returns FlameGraph):
 * {@code curl "http://localhost:10002/prof?event=lock"}</li>
 * </ul>
 * Following event types are supported (default is 'cpu') (NOTE: not all OS'es support all
 * events).<br/>
 * Basic events:
 * <ul>
 * <li>cpu</li>
 * <li>alloc</li>
 * <li>lock</li>
 * <li>wall</li>
 * <li>itimer</li>
 * </ul>
 * Perf events:
 * <ul>
 * <li>L1-dcache-load-misses</li>
 * <li>LLC-load-misses</li>
 * <li>branch-instructions</li>
 * <li>branch-misses</li>
 * <li>bus-cycles</li>
 * <li>cache-misses</li>
 * <li>cache-references</li>
 * <li>context-switches</li>
 * <li>cpu</li>
 * <li>cycles</li>
 * <li>dTLB-load-misses</li>
 * <li>instructions</li>
 * <li>mem:breakpoint</li>
 * <li>page-faults</li>
 * <li>trace:tracepoint</li>
 * </ul>
 */
@InterfaceAudience.Private
public class ProfileServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProfileServlet.class);

  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ALLOWED_METHODS = "GET";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
  private static final String ASYNC_PROFILER_HOME_ENV = "ASYNC_PROFILER_HOME";
  private static final String ASYNC_PROFILER_HOME_SYSTEM_PROPERTY = "async.profiler.home";
  private static final String OLD_PROFILER_SCRIPT = "profiler.sh";
  private static final String PROFILER_SCRIPT = "asprof";
  private static final int DEFAULT_DURATION_SECONDS = 10;
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);
  static final String OUTPUT_DIR = System.getProperty("java.io.tmpdir") + "/prof-output-hbase";

  enum Event {
    CPU("cpu"),
    ALLOC("alloc"),
    LOCK("lock"),
    WALL("wall"),
    ITIMER("itimer"),
    BRANCH_INSTRUCTIONS("branch-instructions"),
    BRANCH_MISSES("branch-misses"),
    BUS_CYCLES("bus-cycles"),
    CACHE_MISSES("cache-misses"),
    CACHE_REFERENCES("cache-references"),
    CONTEXT_SWITCHES("context-switches"),
    CYCLES("cycles"),
    DTLB_LOAD_MISSES("dTLB-load-misses"),
    INSTRUCTIONS("instructions"),
    L1_DCACHE_LOAD_MISSES("L1-dcache-load-misses"),
    LLC_LOAD_MISSES("LLC-load-misses"),
    MEM_BREAKPOINT("mem:breakpoint"),
    PAGE_FAULTS("page-faults"),
    TRACE_TRACEPOINT("trace:tracepoint"),;

    private final String internalName;

    Event(final String internalName) {
      this.internalName = internalName;
    }

    String getInternalName() {
      return internalName;
    }

    static Event fromInternalName(final String name) {
      for (Event event : values()) {
        if (event.getInternalName().equalsIgnoreCase(name)) {
          return event;
        }
      }

      return null;
    }
  }

  private enum Output {
    COLLAPSED,
    FLAMEGRAPH,
    FLAT,
    JFR,
    TRACES,
    TREE
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED",
      justification = "This class is never serialized nor restored.")
  private final transient Lock profilerLock = new ReentrantLock();
  private transient volatile Process process;
  private final String asyncProfilerHome;
  private Integer pid;

  public ProfileServlet() {
    this.asyncProfilerHome = getAsyncProfilerHome();
    this.pid = ProcessUtils.getPid();
    LOG.info("Servlet process PID: {} asyncProfilerHome: {}", pid, asyncProfilerHome);
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
    throws IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), req, resp)) {
      resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      setResponseHeader(resp);
      resp.getWriter().write("Unauthorized: Instrumentation access is not allowed!");
      return;
    }

    // make sure async profiler home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter()
        .write("ASYNC_PROFILER_HOME env is not set.\n\n"
          + "Please ensure the prerequisites for the Profiler Servlet have been installed and the\n"
          + "environment is properly configured. For more information please see\n"
          + "https://hbase.apache.org/book.html#profiler\n");
      return;
    }

    // if pid is explicitly specified, use it else default to current process
    pid = getInteger(req, "pid", pid);

    // if pid is not specified in query param and if current process pid cannot be determined
    if (pid == null) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter()
        .write("'pid' query parameter unspecified or unable to determine PID of current process.");
      return;
    }

    Event event = getEvent(req);
    int duration = getInteger(req, "duration", DEFAULT_DURATION_SECONDS);
    Long interval = getLong(req, "interval");
    Integer jstackDepth = getInteger(req, "jstackdepth", null);
    boolean thread = req.getParameterMap().containsKey("thread");
    boolean simple = req.getParameterMap().containsKey("simple");
    boolean signature = req.getParameterMap().containsKey("signature");
    boolean annotate = req.getParameterMap().containsKey("annotate");
    boolean prependLib = req.getParameterMap().containsKey("prependlib");
    Output output = getOutput(req);
    Double minwidth = getMinWidth(req);
    boolean reverse = req.getParameterMap().containsKey("reverse");

    if (process == null || !process.isAlive()) {
      try {
        int lockTimeoutSecs = 3;
        if (profilerLock.tryLock(lockTimeoutSecs, TimeUnit.SECONDS)) {
          try {
            File outputFile =
              new File(OUTPUT_DIR, "async-prof-pid-" + pid + "-" + event.name().toLowerCase() + "-"
                + ID_GEN.incrementAndGet() + "." + output.name().toLowerCase());

            List<String> cmd = new ArrayList<>();
            Path profilerScriptPath = Paths.get(asyncProfilerHome, "bin", PROFILER_SCRIPT);
            if (!Files.exists(profilerScriptPath)) {
              LOG.info(
                "async-profiler script {} does not exist, fallback to use old script {}(version <= 2.9).",
                PROFILER_SCRIPT, OLD_PROFILER_SCRIPT);
              profilerScriptPath = Paths.get(asyncProfilerHome, OLD_PROFILER_SCRIPT);
            }
            cmd.add(profilerScriptPath.toString());
            cmd.add("-e");
            cmd.add(event.getInternalName());
            cmd.add("-d");
            cmd.add(String.valueOf(duration));
            if (interval != null) {
              cmd.add("-i");
              cmd.add(interval.toString());
            }
            if (jstackDepth != null) {
              cmd.add("-j");
              cmd.add(jstackDepth.toString());
            }
            if (thread) {
              cmd.add("-t");
            }
            if (simple) {
              cmd.add("-s");
            }
            if (signature) {
              cmd.add("-g");
            }
            if (annotate) {
              cmd.add("-a");
            }
            if (prependLib) {
              cmd.add("-l");
            }
            cmd.add("-o");
            cmd.add(output.name().toLowerCase());
            cmd.add("-f");
            cmd.add(outputFile.getAbsolutePath());
            if (minwidth != null) {
              cmd.add("--minwidth");
              cmd.add(minwidth.toString());
            }
            if (reverse) {
              cmd.add("--reverse");
            }

            cmd.add(pid.toString());
            process = ProcessUtils.runCmdAsync(cmd);

            // set response and set refresh header to output location
            setResponseHeader(resp);
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            String relativeUrl = "/prof-output-hbase/" + outputFile.getName();
            resp.getWriter()
              .write("Started [" + event.getInternalName()
                + "] profiling. This page will automatically redirect to " + relativeUrl + " after "
                + duration + " seconds. "
                + "If empty diagram and Linux 4.6+, see 'Basic Usage' section on the Async "
                + "Profiler Home Page, https://github.com/jvm-profiling-tools/async-profiler."
                + "\n\nCommand:\n" + Joiner.on(" ").join(cmd));

            // to avoid auto-refresh by ProfileOutputServlet, refreshDelay can be specified
            // via url param
            int refreshDelay = getInteger(req, "refreshDelay", 0);

            // instead of sending redirect, set auto-refresh so that browsers will refresh
            // with redirected url
            resp.setHeader("Refresh", (duration + refreshDelay) + ";" + relativeUrl);
            resp.getWriter().flush();
          } finally {
            profilerLock.unlock();
          }
        } else {
          setResponseHeader(resp);
          resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          resp.getWriter()
            .write("Unable to acquire lock. Another instance of profiler might be running.");
          LOG.warn(
            "Unable to acquire lock in {} seconds. Another instance of profiler might be running.",
            lockTimeoutSecs);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while acquiring profile lock.", e);
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } else {
      setResponseHeader(resp);
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resp.getWriter().write("Another instance of profiler is already running.");
    }
  }

  private static Integer getInteger(final HttpServletRequest req, final String param,
    final Integer defaultValue) {
    String value = req.getParameter(param);
    if (value != null) {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private static Long getLong(final HttpServletRequest req, final String param) {
    String value = req.getParameter(param);
    if (value != null) {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Double getMinWidth(final HttpServletRequest req) {
    String value = req.getParameter("minwidth");
    if (value != null) {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Event getEvent(final HttpServletRequest req) {
    String eventArg = req.getParameter("event");
    if (eventArg != null) {
      Event event = Event.fromInternalName(eventArg);
      return event == null ? Event.CPU : event;
    }
    return Event.CPU;
  }

  private static Output getOutput(final HttpServletRequest req) {
    String outputArg = req.getParameter("output");
    if (req.getParameter("output") != null) {
      try {
        return Output.valueOf(outputArg.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        return Output.FLAMEGRAPH;
      }
    }
    return Output.FLAMEGRAPH;
  }

  static void setResponseHeader(final HttpServletResponse response) {
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    response.setContentType(CONTENT_TYPE_TEXT);
  }

  static String getAsyncProfilerHome() {
    String asyncProfilerHome = System.getenv(ASYNC_PROFILER_HOME_ENV);
    // if ENV is not set, see if -Dasync.profiler.home=/path/to/async/profiler/home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      asyncProfilerHome = System.getProperty(ASYNC_PROFILER_HOME_SYSTEM_PROPERTY);
    }

    return asyncProfilerHome;
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
          + "https://hbase.apache.org/book.html#profiler\n");
    }

  }

}
