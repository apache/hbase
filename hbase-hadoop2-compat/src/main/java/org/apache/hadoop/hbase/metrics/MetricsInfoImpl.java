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

package org.apache.hadoop.hbase.metrics;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Making implementing metric info a little easier
 */
@InterfaceAudience.Private
class MetricsInfoImpl implements MetricsInfo {
  private final String name, description;

  MetricsInfoImpl(String name, String description) {
    this.name = Preconditions.checkNotNull(name, "name");
    this.description = Preconditions.checkNotNull(description, "description");
  }

  @Override public String name() {
    return name;
  }

  @Override public String description() {
    return description;
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof MetricsInfo) {
      MetricsInfo other = (MetricsInfo) obj;
      return Objects.equal(name, other.name()) &&
          Objects.equal(description, other.description());
    }
    return false;
  }

  @Override public int hashCode() {
    return Objects.hashCode(name, description);
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("name", name).add("description", description)
        .toString();
  }
}
