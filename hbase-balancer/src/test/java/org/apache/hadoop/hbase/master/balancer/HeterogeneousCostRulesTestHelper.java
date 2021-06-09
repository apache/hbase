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
package org.apache.hadoop.hbase.master.balancer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HeterogeneousCostRulesTestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HeterogeneousCostRulesTestHelper.class);

  static final String DEFAULT_RULES_FILE_NAME = "hbase-balancer.rules";

  private HeterogeneousCostRulesTestHelper() {
  }

  /**
   * Create rule file with the given rules.
   * @param file Name of file to write rules into.
   * @return Full file name of the rules file which is <code>dir</code> + DEFAULT_RULES_FILE_NAME.
   */
  static String createRulesFile(String file, final List<String> rules) throws IOException {
    cleanup(file);
    Path path = Files.createFile(FileSystems.getDefault().getPath(file));
    return Files.write(path, rules, StandardCharsets.UTF_8).toString();
  }

  /**
   * Create rule file with empty rules.
   * @param file Name of file to write rules into.
   * @return Full file name of the rules file which is <code>dir</code> + DEFAULT_RULES_FILE_NAME.
   */
  static String createRulesFile(String file) throws IOException {
    return createRulesFile(file, Collections.emptyList());
  }

  static void cleanup(String file) throws IOException {
    try {
      Files.delete(FileSystems.getDefault().getPath(file));
    } catch (NoSuchFileException nsfe) {
      LOG.warn("FileNotFoundException for {}", file, nsfe);
    }
  }
}