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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies {@link ProfilerBackend#detect} fallback behaviour when
 * {@code one.profiler.AsyncProfiler} is absent from the classpath.
 * <p>
 * Each test loads {@code ProfilerBackend} through a custom {@link ClassLoader} that
 * blocks {@code one.profiler.*}, simulating a deployment where the async-profiler JAR
 * was never packaged. This is the exact scenario for users who have async-profiler
 * installed as a native binary ({@code ASYNC_PROFILER_HOME}) but are not allowed to
 * bundle the JAR.
 * <p>
 * The split of {@link LibraryBackend} into its own file is what makes this possible:
 * {@code ProfilerBackend.class} carries no static reference to {@code AsyncProfiler},
 * so the isolated loader can load it without a {@code NoClassDefFoundError}.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestProfilerBackendIsolated {

  @TempDir
  Path tempDir;

  /**
   * When the library is absent AND no home is set, detect() must return null so that
   * HttpServer registers DisabledServlet instead of crashing.
   */
  @Test
  public void testDetectReturnsNullWhenLibraryAbsentAndNoHome() throws Exception {
    ClassLoader isolated = isolatedLoader();
    Method detect = detectMethod(isolated);

    assertNull(detect.invoke(null, (String) null));
    assertNull(detect.invoke(null, ""));
    assertNull(detect.invoke(null, "   "));
  }

  /**
   * User has async-profiler installed as a native binary (ASYNC_PROFILER_HOME set,
   * bin/asprof present) but no JAR on the classpath. detect() must return BinaryBackend.
   */
  @Test
  public void testDetectReturnsBinaryBackendWhenLibraryAbsentButHomeSet() throws Exception {
    // Create a minimal fake profiler home with bin/asprof
    Files.createDirectories(tempDir.resolve("bin"));
    Files.createFile(tempDir.resolve("bin").resolve("asprof"));

    ClassLoader isolated = isolatedLoader();
    Method detect = detectMethod(isolated);

    Object backend = detect.invoke(null, tempDir.toString());
    assertNotNull(backend);
    assertEquals("BinaryBackend", backend.getClass().getSimpleName());
  }

  /**
   * When the library IS on the classpath (normal test classpath), detect() must return
   * LibraryBackend regardless of whether a home is set — library takes priority.
   */
  @Test
  public void testDetectReturnsLibraryBackendWhenLibraryPresent() {
    // Use real classpath — async-profiler JAR is present as optional compile dep in tests
    ProfilerBackend backend = ProfilerBackend.detect(null);
    assertNotNull(backend);
    assertEquals("LibraryBackend", backend.getClass().getSimpleName());
  }

  /**
   * Library present AND home set — LibraryBackend must still win (priority check).
   */
  @Test
  public void testDetectPrefersLibraryWhenBothPresent() throws Exception {
    Files.createDirectories(tempDir.resolve("bin"));
    Files.createFile(tempDir.resolve("bin").resolve("asprof"));

    ProfilerBackend backend = ProfilerBackend.detect(tempDir.toString());
    assertNotNull(backend);
    assertEquals("LibraryBackend", backend.getClass().getSimpleName());
  }

  // ---- helpers ----

  /**
   * Returns a ClassLoader that:
   * - blocks {@code one.profiler.*} entirely (simulates absent async-profiler JAR)
   * - reloads {@code org.apache.hadoop.hbase.http.*} classes fresh (so LibraryBackend
   *   resolves its own imports through this loader and also sees one.profiler.* as absent)
   * - delegates everything else to the parent
   */
  private ClassLoader isolatedLoader() {
    ClassLoader parent = getClass().getClassLoader();
    return new ClassLoader(parent) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("one.profiler.")) {
          throw new ClassNotFoundException("Simulated absent library: " + name);
        }
        // Force fresh load of our http package so LibraryBackend uses this loader
        // (and therefore also sees one.profiler.* as absent when it tries to resolve it)
        if (name.startsWith("org.apache.hadoop.hbase.http.")) {
          Class<?> c = findLoadedClass(name);
          if (c != null) {
            return c;
          }
          // Load bytes from parent, define in this loader
          String path = name.replace('.', '/') + ".class";
          try (java.io.InputStream in = parent.getResourceAsStream(path)) {
            if (in != null) {
              byte[] bytes = in.readAllBytes();
              c = defineClass(name, bytes, 0, bytes.length);
              if (resolve) {
                resolveClass(c);
              }
              return c;
            }
          } catch (java.io.IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        return super.loadClass(name, resolve);
      }
    };
  }

  /**
   * Loads {@code ProfilerBackend} through the given loader and returns its
   * {@code detect(String)} method, made accessible across loader boundaries.
   */
  private Method detectMethod(ClassLoader loader) throws Exception {
    Class<?> backendClass = loader.loadClass("org.apache.hadoop.hbase.http.ProfilerBackend");
    Method m = backendClass.getMethod("detect", String.class);
    m.setAccessible(true);
    return m;
  }
}
