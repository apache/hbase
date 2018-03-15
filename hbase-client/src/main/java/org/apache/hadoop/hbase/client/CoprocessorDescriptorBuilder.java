/**
 *
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
package org.apache.hadoop.hbase.client;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to build the {@link CoprocessorDescriptor}
 */
@InterfaceAudience.Public
public final class CoprocessorDescriptorBuilder {

  public static CoprocessorDescriptor of(String className) {
    return new CoprocessorDescriptorBuilder(className).build();
  }

  public static CoprocessorDescriptorBuilder newBuilder(String className) {
    return new CoprocessorDescriptorBuilder(className);
  }

  private final String className;
  private String jarPath;
  private int priority = Coprocessor.PRIORITY_USER;
  private Map<String, String> properties = new TreeMap();

  public CoprocessorDescriptorBuilder setJarPath(String jarPath) {
    this.jarPath = jarPath;
    return this;
  }

  public CoprocessorDescriptorBuilder setPriority(int priority) {
    this.priority = priority;
    return this;
  }

  public CoprocessorDescriptorBuilder setProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public CoprocessorDescriptorBuilder setProperties(Map<String, String> properties) {
    this.properties.putAll(properties);
    return this;
  }

  public CoprocessorDescriptor build() {
    return new CoprocessorDescriptorImpl(className, jarPath, priority, properties);
  }

  private CoprocessorDescriptorBuilder(String className) {
    this.className = Objects.requireNonNull(className);
  }

  private static final class CoprocessorDescriptorImpl implements CoprocessorDescriptor {
    private final String className;
    private final String jarPath;
    private final int priority;
    private final Map<String, String> properties;

    private CoprocessorDescriptorImpl(String className, String jarPath, int priority,
      Map<String, String> properties) {
      this.className = className;
      this.jarPath = jarPath;
      this.priority = priority;
      this.properties = properties;
    }

    @Override
    public String getClassName() {
      return className;
    }

    @Override
    public Optional<String> getJarPath() {
      return Optional.ofNullable(jarPath);
    }

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public Map<String, String> getProperties() {
      return Collections.unmodifiableMap(properties);
    }

    @Override
    public String toString() {
      return "class:" + className
        + ", jarPath:" + jarPath
        + ", priority:" + priority
        + ", properties:" + properties;
    }
  }
}
