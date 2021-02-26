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
package org.apache.hadoop.hbase.security.visibility.expression;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LeafExpressionNode implements ExpressionNode {
  public static final LeafExpressionNode OPEN_PARAN_NODE = new LeafExpressionNode("(");
  public static final LeafExpressionNode CLOSE_PARAN_NODE = new LeafExpressionNode(")");

  private String identifier;

  public LeafExpressionNode(String identifier) {
    this.identifier = identifier;
  }

  public String getIdentifier() {
    return this.identifier;
  }

  @Override
  public int hashCode() {
    return this.identifier.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LeafExpressionNode) {
      LeafExpressionNode that = (LeafExpressionNode) obj;
      return this.identifier.equals(that.identifier);
    }
    return false;
  }

  @Override
  public String toString() {
    return this.identifier;
  }

  @Override
  public boolean isSingleNode() {
    return true;
  }

  @Override
  public LeafExpressionNode deepClone() {
    LeafExpressionNode clone = new LeafExpressionNode(this.identifier);
    return clone;
  }
}
