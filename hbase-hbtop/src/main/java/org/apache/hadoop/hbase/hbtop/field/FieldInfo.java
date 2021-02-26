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
package org.apache.hadoop.hbase.hbtop.field;

import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Information about a field.
 *
 * This has a {@link Field} itself and additional information (e.g. {@code defaultLength} and
 * {@code displayByDefault}). This additional information is different between the
 * {@link org.apache.hadoop.hbase.hbtop.mode.Mode}s even when the field is the same. That's why the
 * additional information is separated from {@link Field}.
 */
@InterfaceAudience.Private
public class FieldInfo {
  private final Field field;
  private final int defaultLength;
  private final boolean displayByDefault;

  public FieldInfo(Field field, int defaultLength, boolean displayByDefault) {
    this.field = Objects.requireNonNull(field);
    this.defaultLength = defaultLength;
    this.displayByDefault = displayByDefault;
  }

  public Field getField() {
    return field;
  }

  public int getDefaultLength() {
    return defaultLength;
  }

  public boolean isDisplayByDefault() {
    return displayByDefault;
  }
}
