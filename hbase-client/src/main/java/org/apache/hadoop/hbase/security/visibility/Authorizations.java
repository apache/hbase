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
package org.apache.hadoop.hbase.security.visibility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * This class contains visibility labels associated with a Scan/Get deciding which all labeled data
 * current scan/get can access.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Authorizations {

  private List<String> labels;
  public Authorizations(String... labels) {
    this.labels = new ArrayList<String>(labels.length);
    Collections.addAll(this.labels, labels);
  }

  public Authorizations(List<String> labels) {
    this.labels = labels;
  }

  public List<String> getLabels() {
    return Collections.unmodifiableList(this.labels);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (String label: labels) {
      sb.append(label);
      sb.append(' ');
    }
    sb.append(']');
    return sb.toString();
  }

}
