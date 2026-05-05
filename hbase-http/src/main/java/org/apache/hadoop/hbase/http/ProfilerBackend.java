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
import java.util.List;
import org.apache.hadoop.hbase.util.ProcessUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction over async-profiler execution. Implementations handle either the in-process Java API
 * ({@link LibraryBackend}, when the maven dependency is on the classpath) or the external binary
 * ({@link BinaryBackend}, when {@code ASYNC_PROFILER_HOME} is set).
 * <p>
 * This file deliberately contains no import of {@code one.profiler.AsyncProfiler}. That import is
 * isolated in {@link LibraryBackend} so that binary-only deployments never trigger a
 * {@code NoClassDefFoundError} when this class is loaded.
 */
@InterfaceAudience.Private
interface ProfilerBackend {

  /**
   * Executes a profiling start command and returns the profiler's response.
   */
  String executeStart(ProfileServlet.ProfileRequest request, File outputFile) throws IOException;

  /**
   * Executes a profiling stop/dump command.
   */
  String executeStop(ProfileServlet.ProfileRequest request, File outputFile) throws IOException;

  /**
   * Cleans up any resources (e.g. kills a running process). Called on servlet destroy.
   */
  default void destroy() {
  }

  /**
   * Detects which backend is available. Prefers {@link LibraryBackend} over {@link BinaryBackend}.
   * Returns {@code null} if neither is available.
   * <p>
   * When both the library and a binary home are available, {@link LibraryBackend} is preferred
   * and {@code ASYNC_PROFILER_HOME} is ignored.
   * <p>
   * {@link LibraryBackend} is instantiated reflectively so that its class — and therefore
   * {@code one.profiler.AsyncProfiler} — is never loaded on systems where the JAR is absent.
   */
  static ProfilerBackend detect(String asyncProfilerHome) {
    // 1. Try in-process Java API (optional maven dependency).
    //    Use Class.forName to probe without triggering a hard class-load of LibraryBackend,
    //    which would pull in one.profiler.AsyncProfiler and fail on binary-only systems.
    try {
      // Use the classloader that loaded this class so that isolated-classloader tests
      // (which block one.profiler.*) correctly see the library as absent.
      ClassLoader cl = ProfilerBackend.class.getClassLoader();
      Class.forName("one.profiler.AsyncProfiler", false, cl);
      // AsyncProfiler resolved — now safe to load LibraryBackend through the same loader
      return (ProfilerBackend) Class
        .forName("org.apache.hadoop.hbase.http.LibraryBackend", true, cl)
        .getDeclaredConstructor()
        .newInstance();
    } catch (UnsatisfiedLinkError | ReflectiveOperationException e) {
      // library not on classpath or native lib failed to load
    }
    // 2. Try external binary
    if (asyncProfilerHome != null && !asyncProfilerHome.trim().isEmpty()) {
      return new BinaryBackend(asyncProfilerHome);
    }
    return null;
  }
}

/**
 * Backend that invokes the async-profiler binary ({@code asprof} / {@code profiler.sh}) as an
 * external process. Requires {@code ASYNC_PROFILER_HOME} to be set.
 */
@InterfaceAudience.Private
final class BinaryBackend implements ProfilerBackend {

  private static final Logger LOG = LoggerFactory.getLogger(BinaryBackend.class);

  private final String profilerHome;
  private volatile Process process;

  BinaryBackend(String profilerHome) {
    this.profilerHome = profilerHome;
  }

  @Override
  public String executeStart(ProfileServlet.ProfileRequest request, File outputFile)
    throws IOException {
    Integer pid = request.getPid() != null
      ? request.getPid()
      : ProcessUtils.getPid();
    if (pid == null) {
      throw new IOException("Unable to determine PID of current process. "
        + "Set the JVM_PID environment variable or pass '?pid=<pid>' explicitly.");
    }
    List<String> cmd = ProfilerCommandMapper.toCliCommand(request, outputFile, profilerHome, pid);
    process = ProcessUtils.runCmdAsync(cmd);
    return "";
  }

  @Override
  public String executeStop(ProfileServlet.ProfileRequest request, File outputFile)
    throws IOException {
    // The binary runs for the requested duration and exits on its own — nothing to do here.
    return "";
  }

  @Override
  public void destroy() {
    Process p = process;
    if (p != null && p.isAlive()) {
      LOG.info("Destroying async-profiler process on servlet shutdown.");
      p.destroy();
    }
  }
}
