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

import java.util.List;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AttributeTypeHandler {

  /**
   * Get all the attributes that are enabled from the current thread's context
   *
   * @return List of {@link CustomThreadAttribute}
   */
  public List<CustomThreadAttribute> getAllAttributes();

  /**
   * Sets the attributes into current thread's context
   */
  public void setAttribute(String key, Object value);

  /**
   * Clears the attributes from the current thread's context
   */
  public void clearAttribute(String key);

  /**
   * Get an attribute from the current thread's context
   *
   * @return {@link CustomThreadAttribute}
   */
  public CustomThreadAttribute getAttribute(String key);
}
