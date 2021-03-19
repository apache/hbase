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
package org.apache.hadoop.hbase.hbtop.terminal.impl.batch;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.hbtop.terminal.CursorPosition;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalSize;

public class BatchTerminal implements Terminal {

  private static final TerminalPrinter TERMINAL_PRINTER = new BatchTerminalPrinter();

  @Override
  public void clear() {
  }

  @Override
  public void refresh() {
    // Add a new line
    TERMINAL_PRINTER.endOfLine();
  }

  @Nullable
  @Override
  public TerminalSize getSize() {
    return null;
  }

  @Nullable
  @Override
  public TerminalSize doResizeIfNecessary() {
    return null;
  }

  @Nullable
  @Override
  public KeyPress pollKeyPress() {
    return null;
  }

  @Override
  public CursorPosition getCursorPosition() {
    return null;
  }

  @Override
  public void setCursorPosition(int column, int row) {
  }

  @Override
  public void hideCursor() {
  }

  @Override
  public TerminalPrinter getTerminalPrinter(int startRow) {
    return TERMINAL_PRINTER;
  }

  @Override
  public void close() {
  }
}
