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
   * Builds the start command string for the async-profiler Java API.
   * Format: {@code start,event=<event>[,interval=N][,jstackdepth=N][,bufsize=N][,threads][,simple]}
   */
  static String toLibraryStartCommand(ProfileServlet.ProfileRequest request) {
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

  /**
   * Builds the stop command string for the async-profiler Java API.
   * Format: {@code stop,file=<path>,format=<fmt>[,width=N][,height=N][,minwidth=N][,reverse]}
   */
  static String toLibraryStopCommand(ProfileServlet.ProfileRequest request, File outputFile) {
    StringBuilder sb = new StringBuilder("stop");
    sb.append(",file=").append(outputFile.getAbsolutePath());
    sb.append(",format=").append(toFormatString(request.getOutput()));
    appendOption(sb, "width", request.getWidth());
    appendOption(sb, "height", request.getHeight());
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
    cmd.add(request.getOutput().name().toLowerCase());
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
   * Maps the {@link ProfileServlet.Output} enum to the format string used by both backends.
   */
  static String toFormatString(ProfileServlet.Output output) {
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

  private static void appendOption(StringBuilder sb, String key, Object value) {
    if (value != null) {
      sb.append(',').append(key).append('=').append(value);
    }
  }
}
