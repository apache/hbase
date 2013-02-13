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
package org.apache.hadoop.hbase.server.errorhandling;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Base class for an object with a name.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Name {

  private String name;

  public Name(String name) {
    this.name = name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get the name of the class that should be used for logging
   * @return {@link String} prefix for logging
   */
  public String getNamePrefixForLog() {
    return name != null ? "(" + name + ")" : "";
  }

  @Override
  public String toString() {
    return this.name;
  }
}