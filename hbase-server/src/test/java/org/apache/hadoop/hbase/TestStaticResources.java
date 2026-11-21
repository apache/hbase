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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestStaticResources {

  @Test
  public void testStaticResourcesArtifact() throws Exception {
    try (ZipFile resources = new ZipFile(System.getProperty("hbase.test.static.resources.jar"))) {
      for (String resource : new String[] { "css/bootstrap.min.css", "css/hbase.css",
        "js/jquery.min.js", "js/jquery.tablesorter.min.js", "js/parser-date-iso8601.min.js",
        "js/bootstrap.bundle.min.js", "js/vega.min.js", "js/vega-lite.min.js",
        "js/vega-embed.min.js", "js/tab.js", "favicon.ico", "hbase_logo_small.png" }) {
        ZipEntry entry = resources.getEntry("hbase-webapps/static/" + resource);
        assertNotNull(entry, resource);
        assertTrue(entry.getSize() > 0, resource);
      }
      assertTrue(resources.stream()
        .allMatch(entry -> entry.isDirectory() || entry.getName().startsWith("META-INF/")
          || entry.getName().startsWith("hbase-webapps/static/")));
      assertFalse(resources.stream()
        .anyMatch(entry -> entry.getName().endsWith(".class") || entry.getName().endsWith(".jsp")));
      String license = readEntry(resources, "META-INF/LICENSE");
      String notice = readEntry(resources, "META-INF/NOTICE");
      for (String version : System.getProperty("hbase.test.static.resources.versions").split(",")) {
        assertTrue((license + notice).contains(version), version);
      }
      assertTrue(license.contains("Copyright OpenJS Foundation"));
      assertTrue(license.contains("Copyright (c) 2007 Christian Bach"));
      assertTrue(license.contains("Copyright (c) 2015-2023"));
      assertTrue(license.contains("Creative Commons"));
      assertTrue(notice.contains("Bootstrap"));
    }
  }

  private static String readEntry(ZipFile resources, String name) throws Exception {
    ZipEntry entry = resources.getEntry(name);
    assertNotNull(entry, name);
    try (InputStream input = resources.getInputStream(entry)) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
