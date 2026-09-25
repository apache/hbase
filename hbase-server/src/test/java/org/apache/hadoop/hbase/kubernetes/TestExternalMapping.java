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
package org.apache.hadoop.hbase.kubernetes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(SmallTests.TAG)
public class TestExternalMapping {
  public TestExternalMapping() {
  }

  @Test
  public void testNoMapping() {
    Configuration configuration = HBaseConfiguration.create();
    assertThrows(IOException.class, () -> new ExternalMapping(configuration));
  }

  @Test
  public void testMissingMapping() {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.kubernetes.external.mapping", "missing-file.properties");

    assertThrows(IOException.class, () -> new ExternalMapping(configuration));
  }

  private static void writeMapping(Path path, Map<String, String> entries) throws IOException {
    Properties properties = new Properties();
    entries.forEach(properties::setProperty);

    StringWriter writer = new StringWriter();
    properties.store(writer, null);

    Files.writeString(path, writer.toString());
  }

  @Test
  public void testMapping(@TempDir Path tempDir) throws IOException {
    Path mappingPath = tempDir.resolve("mapping.properties");
    writeMapping(mappingPath, Map.of("internal1", "external1", "internal2", "external2"));

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.kubernetes.external.mapping", mappingPath.toString());

    try (ExternalMapping mapping = new ExternalMapping(configuration)) {
      assertEquals("external1", mapping.get("internal1"));
      assertEquals("external2", mapping.get("internal2"));
      assertNull(mapping.get("internal3"));
    }
  }

  @Test
  public void testReload(@TempDir Path tempDirectory) throws IOException, InterruptedException {
    Path mappingPath = tempDirectory.resolve("mapping.properties");
    writeMapping(mappingPath, Map.of("internal1", "external1", "internal2", "external2"));

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.kubernetes.external.mapping", mappingPath.toString());

    try (ExternalMapping mapping = new ExternalMapping(configuration)) {
      assertEquals("external1", mapping.get("internal1"));
      assertEquals("external2", mapping.get("internal2"));

      writeMapping(mappingPath, Map.of("internal3", "external3", "internal4", "external4"));

      /* waiting for event arrival before continuing */
      mapping.getWatchService().take();

      assertEquals("external3", mapping.get("internal3"));
      assertEquals("external4", mapping.get("internal4"));
      assertNull(mapping.get("internal1"));
    }
  }

  @Test
  public void testDelete(@TempDir Path tempDirectory) throws IOException, InterruptedException {
    Path mappingPath = tempDirectory.resolve("mapping.properties");
    writeMapping(mappingPath, Map.of("internal1", "external1", "internal2", "external2"));

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.kubernetes.external.mapping", mappingPath.toString());

    try (ExternalMapping mapping = new ExternalMapping(configuration)) {
      assertEquals("external1", mapping.get("internal1"));
      assertEquals("external2", mapping.get("internal2"));

      Files.delete(mappingPath);

      /* waiting for event arrival before continuing */
      mapping.getWatchService().take();

      /* must use the previous mapping */
      assertEquals("external1", mapping.get("internal1"));
      assertEquals("external2", mapping.get("internal2"));
    }
  }
}
