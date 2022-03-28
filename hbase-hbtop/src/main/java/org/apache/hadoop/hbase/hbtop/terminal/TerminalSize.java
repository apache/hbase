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
 * Terminal dimensions in 2-d space, measured in number of rows and columns.
 */
@InterfaceAudience.Private
public class TerminalSize {
  private final int columns;
  private final int rows;

  public TerminalSize(int columns, int rows) {
    this.columns = columns;
    this.rows = rows;
  }

  public int getColumns() {
    return columns;
  }

  public int getRows() {
    return rows;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TerminalSize)) {
      return false;
    }
    TerminalSize that = (TerminalSize) o;
    return columns == that.columns && rows == that.rows;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, rows);
  }
}
