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
package org.apache.hadoop.hbase.hbtop.terminal.impl;

import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.clearRemainingLine;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.color;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.cursor;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.moveCursor;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.normal;

import java.io.PrintWriter;
import org.apache.hadoop.hbase.hbtop.terminal.Attributes;
import org.apache.hadoop.hbase.hbtop.terminal.CursorPosition;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a buffer of the terminal screen for double-buffering.
 */
@InterfaceAudience.Private
public class ScreenBuffer {
  private int columns;
  private int rows;

  private Cell[][] buffer;
  private Cell[][] physical;

  private boolean cursorVisible;
  private int cursorColumn;
  private int cursorRow;

  public void reallocate(int columns, int rows) {
    buffer = new Cell[columns][rows];
    physical = new Cell[columns][rows];

    for (int row = 0; row < rows; row++) {
      for (int column = 0; column < columns; column++) {
        buffer[column][row] = new Cell();

        physical[column][row] = new Cell();
        physical[column][row].unset();
      }
    }

    this.columns = columns;
    this.rows = rows;
  }

  public void clear() {
    for (int row = 0; row < rows; row++) {
      for (int col = 0; col < columns; col++) {
        buffer[col][row].reset();
      }
    }
  }

  public void flush(PrintWriter output) {
    StringBuilder sb = new StringBuilder();

    sb.append(normal());
    Attributes attributes = new Attributes();
    for (int row = 0; row < rows; row++) {
      flushRow(row, sb, attributes);
    }

    if (cursorVisible && cursorRow >= 0 && cursorColumn >= 0 && cursorRow < rows &&
      cursorColumn < columns) {
      sb.append(cursor(true));
      sb.append(moveCursor(cursorColumn, cursorRow));
    } else {
      sb.append(cursor(false));
    }

    output.write(sb.toString());
    output.flush();
  }

  private void flushRow(int row, StringBuilder sb, Attributes lastAttributes) {
    int lastColumn = -1;
    for (int column = 0; column < columns; column++) {
      Cell cell = buffer[column][row];
      Cell pCell = physical[column][row];

      if (!cell.equals(pCell)) {
        if (lastColumn != column - 1 || lastColumn == -1) {
          sb.append(moveCursor(column, row));
        }

        if (cell.isEndOfLine()) {
          for (int i = column; i < columns; i++) {
            physical[i][row].set(buffer[i][row]);
          }

          sb.append(clearRemainingLine());
          lastAttributes.reset();
          return;
        }

        if (!cell.getAttributes().equals(lastAttributes)) {
          sb.append(color(cell.getForegroundColor(), cell.getBackgroundColor(), cell.isBold(),
            cell.isReverse(), cell.isBlink(), cell.isUnderline()));
        }

        sb.append(cell.getChar());

        lastColumn = column;
        lastAttributes.set(cell.getAttributes());

        physical[column][row].set(cell);
      }
    }
  }

  public CursorPosition getCursorPosition() {
    return new CursorPosition(cursorColumn, cursorRow);
  }

  public void setCursorPosition(int column, int row) {
    cursorVisible = true;
    cursorColumn = column;
    cursorRow = row;
  }

  public void hideCursor() {
    cursorVisible = false;
  }

  public void putString(int column, int row, String string, Attributes attributes) {
    int i = column;
    for (int j = 0; j < string.length(); j++) {
      char ch = string.charAt(j);
      putChar(i, row, ch, attributes);
      i += 1;
      if (i == columns) {
        break;
      }
    }
  }

  public void putChar(int column, int row, char ch, Attributes attributes) {
    if (column >= 0 && column < columns && row >= 0 && row < rows) {
      buffer[column][row].setAttributes(attributes);
      buffer[column][row].setChar(ch);
    }
  }

  public void endOfLine(int column, int row) {
    if (column >= 0 && column < columns && row >= 0 && row < rows) {
      buffer[column][row].endOfLine();
      for (int i = column + 1; i < columns; i++) {
        buffer[i][row].reset();
      }
    }
  }
}
