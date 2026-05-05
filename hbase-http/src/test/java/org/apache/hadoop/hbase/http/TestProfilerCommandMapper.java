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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestProfilerCommandMapper {

  @TempDir
  Path tempDir;

  // ---- Library start command ----

  @Test
  public void testLibraryStartCommandDefaults() {
    ProfileServlet.ProfileRequest req = parseRequest(Collections.emptyMap());
    String cmd = ProfilerCommandMapper.toLibraryStartCommand(req);
    assertTrue(cmd.startsWith("start"));
    assertTrue(cmd.contains("event=cpu"));
    assertFalse(cmd.contains("interval"));
    assertFalse(cmd.contains("threads"));
    assertFalse(cmd.contains("simple"));
  }

  @Test
  public void testLibraryStartCommandAllOptions() {
    Map<String, String[]> flags = new HashMap<>();
    flags.put("thread", new String[] { "" });
    flags.put("simple", new String[] { "" });

    ProfileServlet.ProfileRequest req = parseRequest(flags,
      "event", "alloc", "interval", "1000", "jstackdepth", "256", "bufsize", "100000");
    String cmd = ProfilerCommandMapper.toLibraryStartCommand(req);
    assertTrue(cmd.contains("event=alloc"));
    assertTrue(cmd.contains("interval=1000"));
    assertTrue(cmd.contains("jstackdepth=256"));
    assertTrue(cmd.contains("bufsize=100000"));
    assertTrue(cmd.contains("threads"));
    assertTrue(cmd.contains("simple"));
  }

  // ---- Library stop command ----

  @Test
  public void testLibraryStopCommand() throws IOException {
    Map<String, String[]> flags = new HashMap<>();
    flags.put("reverse", new String[] { "" });
    ProfileServlet.ProfileRequest req = parseRequest(flags,
      "output", "html", "width", "1200", "height", "16", "minwidth", "0.5");

    File outputFile = File.createTempFile("prof", ".html");
    outputFile.deleteOnExit();

    String cmd = ProfilerCommandMapper.toLibraryStopCommand(req, outputFile);
    assertTrue(cmd.startsWith("stop"));
    assertTrue(cmd.contains("file=" + outputFile.getAbsolutePath()));
    assertTrue(cmd.contains("format=html"));
    assertTrue(cmd.contains("width=1200"));
    assertTrue(cmd.contains("height=16"));
    assertTrue(cmd.contains("minwidth=0.5"));
    assertTrue(cmd.contains("reverse"));
  }

  // ---- CLI command ----

  @Test
  public void testCliCommandDefaultScript() throws IOException {
    // Create bin/asprof so the primary script path exists
    Path binDir = Files.createDirectories(tempDir.resolve("bin"));
    Files.createFile(binDir.resolve("asprof"));

    ProfileServlet.ProfileRequest req = parseRequest(Collections.emptyMap(),
      "duration", "30", "output", "html");
    File outputFile = File.createTempFile("prof", ".html");
    outputFile.deleteOnExit();

    List<String> cmd =
      ProfilerCommandMapper.toCliCommand(req, outputFile, tempDir.toString(), 1234);
    assertEquals(tempDir.resolve("bin/asprof").toString(), cmd.get(0));
    assertTrue(cmd.contains("-e"));
    assertTrue(cmd.contains("cpu"));
    assertTrue(cmd.contains("-d"));
    assertTrue(cmd.contains("30"));
    assertTrue(cmd.contains("-o"));
    assertTrue(cmd.contains("html"));
    assertTrue(cmd.contains("-f"));
    assertTrue(cmd.contains(outputFile.getAbsolutePath()));
    assertTrue(cmd.contains("1234"));
  }

  @Test
  public void testCliCommandFallbackToOldScript() throws IOException {
    // Do NOT create bin/asprof — only create profiler.sh as fallback
    Files.createFile(tempDir.resolve("profiler.sh"));

    ProfileServlet.ProfileRequest req = parseRequest(Collections.emptyMap());
    File outputFile = File.createTempFile("prof", ".html");
    outputFile.deleteOnExit();

    List<String> cmd =
      ProfilerCommandMapper.toCliCommand(req, outputFile, tempDir.toString(), 1234);
    assertEquals(tempDir.resolve("profiler.sh").toString(), cmd.get(0));
  }

  @Test
  public void testCliCommandAllOptions() throws IOException {
    Path binDir = Files.createDirectories(tempDir.resolve("bin"));
    Files.createFile(binDir.resolve("asprof"));

    Map<String, String[]> flags = new HashMap<>();
    flags.put("thread", new String[] { "" });
    flags.put("simple", new String[] { "" });
    flags.put("reverse", new String[] { "" });

    ProfileServlet.ProfileRequest req = parseRequest(flags,
      "event", "alloc", "interval", "500", "jstackdepth", "128",
      "bufsize", "50000", "width", "800", "height", "12", "minwidth", "1.0");
    File outputFile = File.createTempFile("prof", ".html");
    outputFile.deleteOnExit();

    List<String> cmd =
      ProfilerCommandMapper.toCliCommand(req, outputFile, tempDir.toString(), 99);
    assertTrue(cmd.contains("-e"));
    assertTrue(cmd.contains("alloc"));
    assertTrue(cmd.contains("-i"));
    assertTrue(cmd.contains("500"));
    assertTrue(cmd.contains("-j"));
    assertTrue(cmd.contains("128"));
    assertTrue(cmd.contains("-b"));
    assertTrue(cmd.contains("50000"));
    assertTrue(cmd.contains("-t"));
    assertTrue(cmd.contains("-s"));
    assertTrue(cmd.contains("--width"));
    assertTrue(cmd.contains("800"));
    assertTrue(cmd.contains("--height"));
    assertTrue(cmd.contains("12"));
    assertTrue(cmd.contains("--minwidth"));
    assertTrue(cmd.contains("1.0"));
    assertTrue(cmd.contains("--reverse"));
  }

  // ---- Format mapping ----

  @Test
  public void testOutputFormatMappingAllValues() {
    assertEquals("summary", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.SUMMARY));
    assertEquals("traces", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.TRACES));
    assertEquals("flat", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.FLAT));
    assertEquals("collapsed",
      ProfilerCommandMapper.toFormatString(ProfileServlet.Output.COLLAPSED));
    assertEquals("tree", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.TREE));
    assertEquals("jfr", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.JFR));
    assertEquals("svg", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.SVG));
    assertEquals("html", ProfilerCommandMapper.toFormatString(ProfileServlet.Output.HTML));
  }

  // ---- helpers ----

  private ProfileServlet.ProfileRequest parseRequest(Map<String, String[]> paramMap,
    String... kvPairs) {
    ProfileServlet servlet = new ProfileServlet(null);
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getParameterMap()).thenReturn(paramMap);
    // defaults
    String[] keys =
      { "pid", "duration", "output", "event", "interval", "jstackdepth", "bufsize", "width",
        "height", "minwidth", "refreshDelay" };
    for (String k : keys) {
      Mockito.when(req.getParameter(k)).thenReturn(null);
    }
    for (int i = 0; i < kvPairs.length; i += 2) {
      Mockito.when(req.getParameter(kvPairs[i])).thenReturn(kvPairs[i + 1]);
    }
    return servlet.parseProfileRequest(req);
  }
}
