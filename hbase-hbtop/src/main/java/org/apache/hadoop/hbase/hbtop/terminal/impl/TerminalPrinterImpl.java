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

import java.util.Objects;
import org.apache.hadoop.hbase.hbtop.terminal.Attributes;
import org.apache.hadoop.hbase.hbtop.terminal.Color;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The implementation of the {@link TerminalPrinter} interface.
 */
@InterfaceAudience.Private
public class TerminalPrinterImpl implements TerminalPrinter {
  private final ScreenBuffer screenBuffer;
  private int row;
  private int column;

  private final Attributes attributes = new Attributes();

  TerminalPrinterImpl(ScreenBuffer screenBuffer, int startRow) {
    this.screenBuffer = Objects.requireNonNull(screenBuffer);
    this.row = startRow;
  }

  @Override
  public TerminalPrinter print(String value) {
    screenBuffer.putString(column, row, value, attributes);
    column += value.length();
    return this;
  }

  @Override
  public TerminalPrinter startHighlight() {
    attributes.setForegroundColor(Color.BLACK);
    attributes.setBackgroundColor(Color.WHITE);
    return this;
  }

  @Override
  public TerminalPrinter stopHighlight() {
    attributes.setForegroundColor(Color.WHITE);
    attributes.setBackgroundColor(Color.BLACK);
    return this;
  }

  @Override
  public TerminalPrinter startBold() {
    attributes.setBold(true);
    return this;
  }

  @Override
  public TerminalPrinter stopBold() {
    attributes.setBold(false);
    return this;
  }

  @Override
  public void endOfLine() {
    screenBuffer.endOfLine(column, row);
    row += 1;
    column = 0;
  }
}
