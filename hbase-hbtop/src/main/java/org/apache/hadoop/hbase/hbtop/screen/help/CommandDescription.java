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
package org.apache.hadoop.hbase.hbtop.screen.help;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a description of a command that we can execute in the top screen.
 */
@InterfaceAudience.Private
public class CommandDescription {

  private final List<String> keys;
  private final String description;

  public CommandDescription(String key, String description) {
    this(Collections.singletonList(Objects.requireNonNull(key)), description);
  }

  public CommandDescription(List<String> keys, String description) {
    this.keys = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(keys)));
    this.description = Objects.requireNonNull(description);
  }

  public List<String> getKeys() {
    return keys;
  }

  public String getDescription() {
    return description;
  }
}
