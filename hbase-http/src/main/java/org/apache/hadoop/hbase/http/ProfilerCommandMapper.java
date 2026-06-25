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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that maps {@link ProfileServlet.ProfileRequest} to async-profiler commands in both
 * the in-process Java API format (comma-separated string) and the CLI format (argument list).
 */
@InterfaceAudience.Private
final class ProfilerCommandMapper {

  private static final Logger LOG = LoggerFactory.getLogger(ProfilerCommandMapper.class);

  private static final String PROFILER_SCRIPT = "asprof";
  private static final String OLD_PROFILER_SCRIPT = "profiler.sh";

  private ProfilerCommandMapper() {
  }

  /**
   * Builds the start command string for the async-profiler Java API. Format:
   * {@code start,event=<event>[,interval=N][,jstackdepth=N][,threads][,simple]}
   * <p>
   * Note: {@code bufsize} is intentionally omitted — it is not a recognized option in the
   * async-profiler 4.x agent grammar and is silently ignored. It remains supported by the
   * BinaryBackend CLI path via {@code -b}.
   */
  static String toLibraryStartCommand(ProfileServlet.ProfileRequest request) {
    StringBuilder sb = new StringBuilder("start");
    sb.append(",event=").append(request.getEvent().getInternalName());
    appendOption(sb, "interval", request.getInterval());
    appendOption(sb, "jstackdepth", request.getJstackDepth());
    if (request.isThread()) {
      sb.append(",threads");
    }
    if (request.isSimple()) {
      sb.append(",simple");
    }
    return sb.toString();
  }

  /**
   * Builds the stop command string for the async-profiler Java API. Format:
   * {@code stop,file=<path>[,<format-token>][,minwidth=N][,reverse]}
   * <p>
   * In async-profiler 4.x the output format is derived from the file extension for html/jfr, and
   * via a bare token (e.g. {@code tree}, {@code flat}) for text-based formats. The {@code format=}
   * key is not recognized. {@code width} and {@code height} are also not recognized by the 4.x
   * agent grammar; they remain supported via the BinaryBackend CLI.
   */
  static String toLibraryStopCommand(ProfileServlet.ProfileRequest request, File outputFile) {
    StringBuilder sb = new StringBuilder("stop");
    sb.append(",file=").append(outputFile.getAbsolutePath());
    String fmt = toFormatString(request.getOutput());
    // html/jfr: format derived from file extension by async-profiler 4.x — no token needed.
    // collapsed/tree/flat/traces/summary: must be passed as a bare token; the .collapsed
    // extension is NOT auto-detected by detectOutputFormat in 4.x.
    if (!fmt.equals("html") && !fmt.equals("jfr")) {
      sb.append(",").append(fmt);
    }
    appendOption(sb, "minwidth", request.getMinwidth());
    if (request.isReverse()) {
      sb.append(",reverse");
    }
    return sb.toString();
  }

  /**
   * Builds the CLI argument list for invoking the async-profiler binary (asprof / profiler.sh).
   * Locates the script under {@code <profilerHome>/bin/asprof}, falling back to
   * {@code <profilerHome>/profiler.sh} for older installations.
   */
  static List<String> toCliCommand(ProfileServlet.ProfileRequest request, File outputFile,
    String profilerHome, Integer pid) {
    List<String> cmd = new ArrayList<>();
    Path profilerScriptPath = Paths.get(profilerHome, "bin", PROFILER_SCRIPT);
    if (!Files.exists(profilerScriptPath)) {
      LOG.info("async-profiler script {} does not exist, falling back to {}(version <= 2.9).",
        PROFILER_SCRIPT, OLD_PROFILER_SCRIPT);
      profilerScriptPath = Paths.get(profilerHome, OLD_PROFILER_SCRIPT);
    }
    cmd.add(profilerScriptPath.toString());
    cmd.add("-e");
    cmd.add(request.getEvent().getInternalName());
    cmd.add("-d");
    cmd.add(String.valueOf(request.getDuration()));
    cmd.add("-o");
    cmd.add(toFormatString(request.getOutput()));
    cmd.add("-f");
    cmd.add(outputFile.getAbsolutePath());
    if (request.getInterval() != null) {
      cmd.add("-i");
      cmd.add(request.getInterval().toString());
    }
    if (request.getJstackDepth() != null) {
      cmd.add("-j");
      cmd.add(request.getJstackDepth().toString());
    }
    if (request.getBufsize() != null) {
      cmd.add("-b");
      cmd.add(request.getBufsize().toString());
    }
    if (request.isThread()) {
      cmd.add("-t");
    }
    if (request.isSimple()) {
      cmd.add("-s");
    }
    if (request.getWidth() != null) {
      cmd.add("--width");
      cmd.add(request.getWidth().toString());
    }
    if (request.getHeight() != null) {
      cmd.add("--height");
      cmd.add(request.getHeight().toString());
    }
    if (request.getMinwidth() != null) {
      cmd.add("--minwidth");
      cmd.add(request.getMinwidth().toString());
    }
    if (request.isReverse()) {
      cmd.add("--reverse");
    }
    cmd.add(pid.toString());
    return cmd;
  }

  /**
   * Maps the {@link ProfileServlet.Output} enum to the format string used by both backends. Logs a
   * deprecation warning when SVG is requested (it was removed in async-profiler 2.0, see
   * HBASE-25685). Use {@link #toFileExtension} when only the file extension is needed and the
   * warning has already been emitted.
   */
  static String toFormatString(ProfileServlet.Output output) {
    if (output == ProfileServlet.Output.SVG) {
      LOG.warn("output=svg is obsolete (HBASE-25685); redirecting to html (FlameGraph). "
        + "Use output=html explicitly.");
    }
    return toFileExtension(output);
  }

  /**
   * Maps the {@link ProfileServlet.Output} enum to a file extension / format token without emitting
   * any log warnings. SVG is silently remapped to {@code "html"}.
   */
  static String toFileExtension(ProfileServlet.Output output) {
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
        // SVG was dropped in async-profiler 2.0 (HBASE-25685) and hard-errors in 4.x.
        return "html";
      case HTML:
      default:
        return "html";
    }
  }

  private static void appendOption(StringBuilder sb, String key, Object value) {
    if (value != null) {
      sb.append(',').append(key).append('=').append(value);
    }
  }
}
