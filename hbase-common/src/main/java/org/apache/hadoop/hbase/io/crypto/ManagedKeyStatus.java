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
package org.apache.hadoop.hbase.io.crypto;

import org.apache.yetus.audience.InterfaceAudience;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum of Managed key status. It is used to indicate the status of managed custodian keys.
 */
@InterfaceAudience.Public
public enum ManagedKeyStatus {
  /** Represents the active status of a managed key. */
  ACTIVE((byte) 1),
  /** Represents the inactive status of a managed key. */
  INACTIVE((byte) 2),
  /** Represents the retrieval failure status of a managed key. */
  FAILED((byte) 3),
  /** Represents the disabled status of a managed key. */
  DISABLED((byte) 4),
  ;

  private static Map<Byte, ManagedKeyStatus> lookupByVal;

  private final byte val;

  private ManagedKeyStatus(byte val) {
    this.val = val;
  }

  /**
   * Returns the numeric value of the managed key status.
   * @return byte value
   */
  public byte getVal() {
    return val;
  }

  /**
   * Returns the ManagedKeyStatus for the given numeric value.
   * @param val The numeric value of the desired ManagedKeyStatus
   * @return The ManagedKeyStatus corresponding to the given value
   */
  public static ManagedKeyStatus forValue(byte val) {
    if (lookupByVal == null) {
      Map<Byte, ManagedKeyStatus> tbl = new HashMap<>();
      for (ManagedKeyStatus e: ManagedKeyStatus.values()) {
        tbl.put(e.getVal(), e);
      }
      lookupByVal = tbl;
    }
    return lookupByVal.get(val);
  }
}
