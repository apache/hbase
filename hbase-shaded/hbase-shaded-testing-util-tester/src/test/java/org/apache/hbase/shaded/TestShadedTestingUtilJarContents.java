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
package org.apache.hbase.shaded;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestShadedTestingUtilJarContents {

  @Test
  public void testNoBundledJUnitLibraries() throws Exception {
    File path =
      new File(HBaseTestingUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    try (JarFile jar = new JarFile(path)) {
      JarEntry unexpectedEntry = jar.stream().filter(entry -> {
        String name = entry.getName();
        return name.startsWith("META-INF/services/org.junit.platform.") || name.endsWith(".class")
          && (name.startsWith("org/junit/") && !name.equals("org/junit/Assert.class")
            || name.startsWith("org/opentest4j/") || name.startsWith("org/apiguardian/")
            || name.matches("META-INF/versions/[0-9]+/org/junit/.*"));
      }).findFirst().orElse(null);
      assertNull(unexpectedEntry, () -> "Unexpected bundled entry: " + unexpectedEntry);
      // HBase's JUnit 4 shim is needed by bundled Hadoop test utilities.
      assertNotNull(jar.getEntry("org/junit/Assert.class"));
      assertNotNull(jar.getEntry("META-INF/services/org.junit.jupiter.api.extension.Extension"));
    }
  }
}
