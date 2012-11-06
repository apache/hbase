
/*
 * Copyright The Apache Software Foundation.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;

/**
 * <p>
 * This is a wrapper around the {@link KeyValue} to insure the user is disabled
 * from modifying other fields in the KeyValue except the value. Please don't
 * try to modify the visibility of keyValue from private to public, or generate
 * a setter, you will introduce huge harms.
 * </p>
 *
 *
 * <p>
 * We put this class in regionserver package since we want to be able to access
 * the getKeyValue() method in {@link Store} and we want to restricted custom
 * Compaction Hooks to access the method.
 * </p>
 */
public class RestrictedKeyValue {
  private KeyValue keyValue;

  public RestrictedKeyValue(KeyValue keyValue) {
    this.keyValue = keyValue;
  }

  /**
   * If you don't call this method, the KV will stay unchanged.
   * @param newValue
   */
  public void modifyValue(byte[] newValue) {
    this.keyValue = keyValue.modifyValueAndClone(newValue);
  }

  public byte[] getValue() {
    return keyValue.getValue();
  }

  /** don't change the modifier */
  KeyValue getKeyValue() {
    return keyValue;
  }

  public byte[] getRow() {
    return keyValue.getRow();
  }

  public byte[] getFamily() {
    return keyValue.getFamily();
  }

  public byte[] getQualifier() {
    return keyValue.getQualifier();
  }

  public long getTimestamp() {
    return keyValue.getTimestamp();
  }
}
