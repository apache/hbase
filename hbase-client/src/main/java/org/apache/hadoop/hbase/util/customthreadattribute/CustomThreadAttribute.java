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
package org.apache.hadoop.hbase.util.customthreadattribute;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * This class defines a custom thread attribute
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CustomThreadAttribute {
  private String key;
  private Object value;
  private AttributeType type;

  /**
   * Constructor for initializing a custom thread attribute
   * @param key An attribute's Key
   * @param value An attribute's Value
   * @param type An attribute's Type
   */
  public CustomThreadAttribute(String key, Object value, AttributeType type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  /**
   * @return Attribute's Key
   */
  public String getKey() {
    return key;
  }

  /**
   * @return Attribute's Value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @return Attribute's Type
   */
  public AttributeType getType() {
    return type;
  }

  /**
   * set Attribute's Type
   */
  public void setType(AttributeType type) {
    this.type = type;
  }
}
