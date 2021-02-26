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
package org.apache.hadoop.hbase.hbtop.terminal;

import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * A 2-d position in 'terminal space'.
 */
@InterfaceAudience.Private
public class CursorPosition {
  private final int column;
  private final int row;

  public CursorPosition(int column, int row) {
    this.column = column;
    this.row = row;
  }

  public int getColumn() {
    return column;
  }

  public int getRow() {
    return row;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CursorPosition)) {
      return false;
    }
    CursorPosition that = (CursorPosition) o;
    return column == that.column && row == that.row;
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, row);
  }
}
