/**
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
package org.apache.hadoop.hbase.logging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.LogManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Setup {@link SLF4JBridgeHandler}.
 * <p/>
 * Set the system property {@code java.util.logging.config.class} to this class to initialize the
 * direction for java.util.logging to slf4j.
 */
@InterfaceAudience.Private
public class JulToSlf4jInitializer {

  private static final String PROPERTIES = "handlers=" + SLF4JBridgeHandler.class.getName();

  public JulToSlf4jInitializer() throws IOException {
    LogManager.getLogManager()
      .readConfiguration(new ByteArrayInputStream(PROPERTIES.getBytes(StandardCharsets.UTF_8)));
  }
}
