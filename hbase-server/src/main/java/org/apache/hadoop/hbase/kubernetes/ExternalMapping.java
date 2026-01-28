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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ExternalMapping implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalMapping.class);

  private static final String MAPPING_KEY = "hbase.kubernetes.external.mapping";

  private final Path mappingPath;
  private final WatchService watchService;
  private final WatchKey watchKey;

  private volatile Properties mapping;

  public ExternalMapping(Configuration configuration) throws IOException {
    String mappingFile = configuration.get(MAPPING_KEY);

    if (mappingFile == null) {
      throw new IOException(ExternalKubernetesCoprocessor.class.getSimpleName()
        + " is in use, but missing '" + MAPPING_KEY + "' configuration property.");
    }

    mappingPath = Paths.get(mappingFile);

    read();

    watchService = mappingPath.getFileSystem().newWatchService();

    try {
      Path parent = mappingPath.getParent();
      watchKey = parent.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
    } catch (Throwable e) {
      watchService.close();
      throw e;
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests")
  protected WatchService getWatchService() {
    return watchService;
  }

  private void read() throws IOException {
    LOG.debug("Reading external mapping file '{}'.", mappingPath);

    Properties mapping = new Properties();

    try (Reader reader = Files.newBufferedReader(mappingPath, StandardCharsets.UTF_8)) {
      mapping.load(reader);
    }

    this.mapping = mapping;
  }

  private void processEvents() {
    boolean changed = false;

    for (WatchEvent<?> event : watchKey.pollEvents()) {
      Path path = (Path) event.context();

      if (mappingPath.getFileName().equals(path.getFileName())) {
        changed = true;
        break;
      }
    }

    watchKey.reset();

    if (!changed) {
      return;
    }

    LOG.debug("External mapping file '{}' has changed.", mappingPath);

    if (!Files.isRegularFile(mappingPath)) {
      LOG.warn("Could not refresh external mapping file '{}',"
        + "because it is not a regular file anymore. Using previous mapping.", mappingPath);
      return;
    }

    try {
      read();
    } catch (IOException e) {
      LOG.warn("Could not refresh external mapping file '{}'. Using previous mapping.", mappingPath,
        e);
    }
  }

  public String get(String key) {
    processEvents();

    if (mapping == null) {
      return null;
    }

    return mapping.getProperty(key);
  }

  @Override
  public void close() throws IOException {
    watchService.close();
  }
}
