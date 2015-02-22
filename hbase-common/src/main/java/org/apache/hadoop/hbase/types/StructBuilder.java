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
package org.apache.hadoop.hbase.types;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A helper for building {@link Struct} instances.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StructBuilder {

  protected final List<DataType<?>> fields = new ArrayList<DataType<?>>();

  /**
   * Create an empty {@code StructBuilder}.
   */
  public StructBuilder() {}

  /**
   * Append {@code field} to the sequence of accumulated fields.
   */
  public StructBuilder add(DataType<?> field) { fields.add(field); return this; }

  /**
   * Retrieve the {@link Struct} represented by {@code this}.
   */
  public Struct toStruct() { return new Struct(fields.toArray(new DataType<?>[fields.size()])); }

  /**
   * Reset the sequence of accumulated fields.
   */
  public StructBuilder reset() { fields.clear(); return this; }
}
