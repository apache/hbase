/*
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
package org.apache.hadoop.hbase.io.hfile.cache;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Decision describing the cached representation for an admitted block.
 */
@InterfaceAudience.Private
public final class RepresentationDecision {

  private static final RepresentationDecision CURRENT_HBASE_DEFAULT =
    new RepresentationDecision(RepresentationKind.CURRENT_HBASE_DEFAULT);

  private final RepresentationKind kind;

  private RepresentationDecision(RepresentationKind kind) {
    this.kind = kind;
  }

  /**
   * Preserves the representation currently produced by existing HBase code paths.
   * @return current HBase default representation decision
   */
  public static RepresentationDecision currentHBaseDefault() {
    return CURRENT_HBASE_DEFAULT;
  }

  /**
   * Returns a representation decision for the supplied representation kind.
   * @param kind representation kind
   * @return representation decision
   */
  public static RepresentationDecision of(RepresentationKind kind) {
    return kind == RepresentationKind.CURRENT_HBASE_DEFAULT
      ? CURRENT_HBASE_DEFAULT
      : new RepresentationDecision(kind);
  }

  /**
   * Returns the selected representation kind.
   * @return representation kind
   */
  public RepresentationKind getKind() {
    return kind;
  }
}
