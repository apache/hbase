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
import org.apache.hadoop.hbase.util.Bytes;

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
   * Create RestrictedKeyValue deep copy
   * @param rkv
   */
  public RestrictedKeyValue(RestrictedKeyValue rkv) {
    this.keyValue = rkv.getKeyValue().clone();
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

  /**
   * Compare how many bytes the value of this keyvalue is taking comparing to
   * the other. Negative value is good, positive is bad!
   *
   * @param other
   * @return
   */
  public int differenceInBytes(KeyValue other) {
    if (keyValue == null && other == null) {
      return 0;
    } else if (keyValue == null) {
      return -other.getLength();
    } else if (other == null) {
      return keyValue.getLength();
    } else {
      return this.keyValue.getValueLength() - other.getValueLength();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((keyValue == null) ? 0 : keyValue.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RestrictedKeyValue other = (RestrictedKeyValue) obj;
    if (keyValue == null) {
      if (other.keyValue != null)
        return false;
    } else if (!keyValue.equals(other.keyValue)) {
      //this just compares the key part (ignoring the value)
      return false;
    } else if (Bytes.BYTES_RAWCOMPARATOR.compare(keyValue.getBuffer(),
        keyValue.getValueOffset(), keyValue.getValueLength(),
        other.keyValue.getBuffer(), other.keyValue.getValueOffset(),
        other.keyValue.getValueLength()) != 0) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "RestrictedKeyValue [keyValue=" + keyValue + "/value=" + Bytes.toString(keyValue.getValue()) + "]";
  }

}
