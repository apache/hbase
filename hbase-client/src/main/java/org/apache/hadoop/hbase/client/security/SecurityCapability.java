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
package org.apache.hadoop.hbase.client.security;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Available security capabilities
 */
@InterfaceAudience.Public
public enum SecurityCapability {
  // Note to implementors: These must match the numbering of Capability values in MasterProtos
  SIMPLE_AUTHENTICATION(0),
  SECURE_AUTHENTICATION(1),
  AUTHORIZATION(2),
  CELL_AUTHORIZATION(3),
  CELL_VISIBILITY(4);

  private final int value;

  public int getValue() {
    return value;
  }

  public String getName() {
    return toString();
  }

  private SecurityCapability(int value) {
    this.value = value;
  }

  public static SecurityCapability valueOf(int value) {
    switch (value) {
      case 0: return SIMPLE_AUTHENTICATION;
      case 1: return SECURE_AUTHENTICATION;
      case 2: return AUTHORIZATION;
      case 3: return CELL_AUTHORIZATION;
      case 4: return CELL_VISIBILITY;
      default:
        throw new IllegalArgumentException("Unknown SecurityCapability value " + value);
    }
  }
}
