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
package org.apache.hadoop.hbase.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public enum BloomType {
  /**
   * Bloomfilters disabled
   */
  NONE,
  /**
   * Bloom enabled with Table row as Key
   */
  ROW,
  /**
   * Bloom enabled with Table row &amp; column (family+qualifier) as Key
   */
  ROWCOL,
  /**
   * Bloom enabled with Table row prefix as Key, specify the length of the prefix
   */
  ROWPREFIX_FIXED_LENGTH,
  /**
   * Ribbon filter enabled with Table row as Key
   */
  RIBBON_ROW,
  /**
   * Ribbon filter enabled with Table row &amp; column (family+qualifier) as Key
   */
  RIBBON_ROWCOL,
  /**
   * Ribbon filter enabled with Table row prefix as Key, specify the length of the prefix
   */
  RIBBON_ROWPREFIX_FIXED_LENGTH;

  /** Returns true if Ribbon filter, false if Bloom filter or NONE */
  public boolean isRibbon() {
    return this == RIBBON_ROW || this == RIBBON_ROWCOL || this == RIBBON_ROWPREFIX_FIXED_LENGTH;
  }

  /**
   * Returns the base key type (ROW, ROWCOL, or ROWPREFIX_FIXED_LENGTH) for this filter type.
   * @return the base BloomType for key extraction
   */
  public BloomType toBaseType() {
    return switch (this) {
      case RIBBON_ROW -> ROW;
      case RIBBON_ROWCOL -> ROWCOL;
      case RIBBON_ROWPREFIX_FIXED_LENGTH -> ROWPREFIX_FIXED_LENGTH;
      default -> this;
    };
  }

  /** Returns true if ROW or RIBBON_ROW */
  public boolean isRowOnly() {
    return this == ROW || this == RIBBON_ROW;
  }

  /** Returns true if ROWCOL or RIBBON_ROWCOL */
  public boolean isRowCol() {
    return this == ROWCOL || this == RIBBON_ROWCOL;
  }

  /** Returns true if ROWPREFIX_FIXED_LENGTH or RIBBON_ROWPREFIX_FIXED_LENGTH */
  public boolean isRowPrefixFixedLength() {
    return this == ROWPREFIX_FIXED_LENGTH || this == RIBBON_ROWPREFIX_FIXED_LENGTH;
  }
}
