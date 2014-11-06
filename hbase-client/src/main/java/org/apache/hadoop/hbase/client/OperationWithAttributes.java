/*
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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class OperationWithAttributes extends Operation implements Attributes {
  // An opaque blob of attributes
  private Map<String, byte[]> attributes;

  // used for uniquely identifying an operation
  public static final String ID_ATRIBUTE = "_operation.attributes.id";

  @Override
  public OperationWithAttributes setAttribute(String name, byte[] value) {
    if (attributes == null && value == null) {
      return this;
    }

    if (attributes == null) {
      attributes = new HashMap<String, byte[]>();
    }

    if (value == null) {
      attributes.remove(name);
      if (attributes.isEmpty()) {
        this.attributes = null;
      }
    } else {
      attributes.put(name, value);
    }
    return this;
  }

  @Override
  public byte[] getAttribute(String name) {
    if (attributes == null) {
      return null;
    }

    return attributes.get(name);
  }

  @Override
  public Map<String, byte[]> getAttributesMap() {
    if (attributes == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(attributes);
  }

  protected long getAttributeSize() {
    long size = 0;
    if (attributes != null) {
      size += ClassSize.align(this.attributes.size() * ClassSize.MAP_ENTRY);
      for(Map.Entry<String, byte[]> entry : this.attributes.entrySet()) {
        size += ClassSize.align(ClassSize.STRING + entry.getKey().length());
        size += ClassSize.align(ClassSize.ARRAY + entry.getValue().length);
      }
    }
    return size;
  }

  /**
   * This method allows you to set an identifier on an operation. The original
   * motivation for this was to allow the identifier to be used in slow query
   * logging, but this could obviously be useful in other places. One use of
   * this could be to put a class.method identifier in here to see where the
   * slow query is coming from.
   * @param id
   *          id to set for the scan
   */
  public OperationWithAttributes setId(String id) {
    setAttribute(ID_ATRIBUTE, Bytes.toBytes(id));
    return this;
  }

  /**
   * This method allows you to retrieve the identifier for the operation if one
   * was set.
   * @return the id or null if not set
   */
  public String getId() {
    byte[] attr = getAttribute(ID_ATRIBUTE);
    return attr == null? null: Bytes.toString(attr);
  }
}
