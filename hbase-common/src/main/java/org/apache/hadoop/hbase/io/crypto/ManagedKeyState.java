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

import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Enum of Managed key status. It is used to indicate the status of managed custodian keys.
 */
@InterfaceAudience.Public
public enum ManagedKeyState {
  /** Represents the active status of a managed key. */
  ACTIVE((byte) 1),
  /** Represents the inactive status of a managed key. */
  INACTIVE((byte) 2),
  /** Represents the retrieval failure status of a managed key. */
  FAILED((byte) 3),
  /** Represents the disabled status of a managed key. */
  DISABLED((byte) 4),;

  private static Map<Byte, ManagedKeyState> lookupByVal;

  private final byte val;

  private ManagedKeyState(byte val) {
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
   * Returns the ManagedKeyState for the given numeric value.
   * @param val The numeric value of the desired ManagedKeyState
   * @return The ManagedKeyState corresponding to the given value
   */
  public static ManagedKeyState forValue(byte val) {
    if (lookupByVal == null) {
      Map<Byte, ManagedKeyState> tbl = new HashMap<>();
      for (ManagedKeyState e : ManagedKeyState.values()) {
        tbl.put(e.getVal(), e);
      }
      lookupByVal = tbl;
    }
    return lookupByVal.get(val);
  }

  /**
   * This is used to determine if a key is usable for encryption/decryption.
   * @param state The key state to check
   * @return true if the key state is ACTIVE or INACTIVE, false otherwise
   */
  public static boolean isUsable(ManagedKeyState state) {
    return state == ACTIVE || state == INACTIVE;
  }
}
