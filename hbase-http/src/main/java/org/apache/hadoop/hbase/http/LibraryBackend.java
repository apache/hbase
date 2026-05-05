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
import one.profiler.AsyncProfiler;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Backend that uses the async-profiler Java API (in-process). Requires the
 * {@code tools.profiler:async-profiler} JAR and its native library on the classpath.
 * <p>
 * This class is intentionally isolated in its own file so that the JVM never loads
 * {@code one.profiler.AsyncProfiler} on systems where the JAR is absent. It is only
 * instantiated reflectively from {@link ProfilerBackend#detect} after confirming the
 * class is resolvable.
 */
@InterfaceAudience.Private
final class LibraryBackend implements ProfilerBackend {

  @Override
  public String executeStart(ProfileServlet.ProfileRequest request, File outputFile)
    throws IOException {
    String cmd = ProfilerCommandMapper.toLibraryStartCommand(request);
    return AsyncProfiler.getInstance().execute(cmd);
  }

  @Override
  public String executeStop(ProfileServlet.ProfileRequest request, File outputFile)
    throws IOException {
    String cmd = ProfilerCommandMapper.toLibraryStopCommand(request, outputFile);
    return AsyncProfiler.getInstance().execute(cmd);
  }

  @Override
  public void destroy() {
    try {
      AsyncProfiler.getInstance().execute("stop");
    } catch (Exception e) {
      // ignored — profiler may not have been active at shutdown time
    }
  }
}
