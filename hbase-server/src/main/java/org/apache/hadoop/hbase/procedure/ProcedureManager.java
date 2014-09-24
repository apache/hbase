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
package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedureManager {

  /**
   * Return the unique signature of the procedure. This signature uniquely
   * identifies the procedure. By default, this signature is the string used in
   * the procedure controller (i.e., the root ZK node name for the procedure)
   */
  public abstract String getProcedureSignature();

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ProcedureManager)) {
      return false;
    }
    ProcedureManager other = (ProcedureManager)obj;
    return this.getProcedureSignature().equals(other.getProcedureSignature());
  }

  @Override
  public int hashCode() {
    return this.getProcedureSignature().hashCode();
  }
}
