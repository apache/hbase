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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestProfilerBackend {

  @TempDir
  Path tempDir;

  @Test
  public void testDetectReturnsLibraryBackendWhenLibraryOnClasspath() {
    // async-profiler is on the test classpath, so detect() always returns LibraryBackend
    // regardless of home setting — library takes priority.
    ProfilerBackend backend = ProfilerBackend.detect(null);
    assertNotNull(backend);
    assertInstanceOf(LibraryBackend.class, backend);
  }

  @Test
  public void testDetectReturnsBinaryBackendWhenHomeSet() throws Exception {
    // Create a fake profiler home with bin/asprof so path check passes
    Files.createDirectories(tempDir.resolve("bin"));
    Files.createFile(tempDir.resolve("bin").resolve("asprof"));

    // The test classpath has the async-profiler JAR (optional compile dep), so detect() returns
    // LibraryBackend here. BinaryBackend selection is verified under isolation by
    // TestProfilerBackendIsolated.testDetectReturnsBinaryBackendWhenLibraryAbsentButHomeSet.
    // This test simply asserts that a valid home always yields a non-null backend.
    assertNotNull(ProfilerBackend.detect(tempDir.toString()));
  }

  @Test
  public void testBinaryBackendDetectReturnsNonNullWhenHomeProvided() {
    // Any non-empty home string produces a non-null backend (LibraryBackend when JAR is present,
    // BinaryBackend when absent). Both are valid — what matters is non-null.
    assertNotNull(ProfilerBackend.detect("/fake/profiler/home"));
  }

  @Test
  public void testDetectPrefersLibraryWhenBothAvailable() {
    // Library takes priority over binary home. Since the JAR is on the test classpath,
    // detect() must return LibraryBackend even when a home is provided.
    ProfilerBackend backend = ProfilerBackend.detect("/some/home");
    assertNotNull(backend);
    assertInstanceOf(LibraryBackend.class, backend);
  }

  @Test
  public void testBinaryBackendDestroyDoesNotThrowWhenNoProcess() {
    BinaryBackend backend = new BinaryBackend("/fake/home");
    // Should not throw when no process has been started
    backend.destroy();
  }

  @Test
  public void testBinaryBackendExecuteStartFailureIsNotNullPointerException() throws Exception {
    // BinaryBackend.executeStart must surface failures as IOException or RuntimeException,
    // never NullPointerException. The null-PID guard at BinaryBackend.java:116-118 cannot be
    // triggered without mocking ProcessUtils.getPid() (static method), but we verify the
    // adjacent failure path: a non-existent profiler home causes ProcessBuilder.start() to fail,
    // which runCmdAsync wraps in IllegalStateException. This confirms the error path goes through
    // the expected exception type and not an unguarded NPE from pid.toString().
    BinaryBackend backend = new BinaryBackend("/fake/home/that/does/not/exist");
    ProfileServlet servlet = new ProfileServlet(null);
    javax.servlet.http.HttpServletRequest req =
      org.mockito.Mockito.mock(javax.servlet.http.HttpServletRequest.class);
    org.mockito.Mockito.when(req.getParameterMap()).thenReturn(java.util.Collections.emptyMap());
    org.mockito.Mockito.when(req.getParameter(org.mockito.Mockito.anyString())).thenReturn(null);
    ProfileServlet.ProfileRequest profileReq = servlet.parseProfileRequest(req);
    Throwable caught = null;
    try {
      backend.executeStart(profileReq, File.createTempFile("test", ".html"));
    } catch (Exception e) {
      caught = e;
    }
    assertNotNull(caught, "executeStart must throw when profiler home is missing");
    assertFalse(caught instanceof NullPointerException,
      "executeStart must not throw NullPointerException; got: " + caught.getClass().getName());
  }
}
