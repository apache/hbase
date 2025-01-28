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

@InterfaceAudience.Public
public enum PBEKeyStatus {
  ACTIVE((byte) 1),
  INACTIVE((byte) 2),
  FAILED((byte) 3),
  DISABLED((byte) 4),
  ;

  private static Map<Byte, PBEKeyStatus> lookupByVal;

  private final byte val;

  PBEKeyStatus(byte val) {
    this.val = val;
  }

  public byte getVal() {
    return val;
  }

  public static PBEKeyStatus forValue(byte val) {
    if (lookupByVal == null) {
      Map<Byte, PBEKeyStatus> tbl = new HashMap<>();
      for (PBEKeyStatus e: PBEKeyStatus.values()) {
        tbl.put(e.getVal(), e);
      }
      lookupByVal = tbl;
    }
    return lookupByVal.get(val);
  }
}
