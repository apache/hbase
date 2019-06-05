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
import org.apache.commons.lang3.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Utility class for printing to terminal.
 */
@InterfaceAudience.Private
public class TerminalPrinter {
  private final Terminal terminal;
  private final int terminalColumns;
  private int row;
  private int column;

  TerminalPrinter(Terminal terminal, int terminalColumns, int startRow) {
    this.terminal = Objects.requireNonNull(terminal);
    this.terminalColumns = terminalColumns;
    this.row = startRow;
  }

  public TerminalPrinter print(Object value) {
    String string = value.toString();
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter print(char value) {
    String string = Character.toString(value);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter print(short value) {
    String string = Short.toString(value);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter print(int value) {
    String string = Integer.toString(value);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter print(long value) {
    String string = Long.toString(value);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter print(float value) {
    String string = Float.toString(value);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter print(double value) {
    String string = Double.toString(value);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter printFormat(String format, Object... args) {
    String string = String.format(format, args);
    terminal.print(column, row, string);
    column += string.length();
    return this;
  }

  public TerminalPrinter setForegroundColor(TextColor textColor) {
    terminal.setForegroundColor(textColor);
    return this;
  }

  public TerminalPrinter setBackgroundColor(TextColor textColor) {
    terminal.setBackgroundColor(textColor);
    return this;
  }

  public TerminalPrinter startHighlight() {
    setForegroundColor(TextColor.BLACK);
    return setBackgroundColor(TextColor.WHITE);
  }

  public TerminalPrinter stopHighlight() {
    setForegroundColor(TextColor.DEFAULT);
    return setBackgroundColor(TextColor.DEFAULT);
  }

  public TerminalPrinter enableModifiers(SGR... modifiers) {
    terminal.enableModifiers(modifiers);
    return this;
  }

  public TerminalPrinter disableModifiers(SGR... modifiers) {
    terminal.disableModifiers(modifiers);
    return this;
  }

  public TerminalPrinter startBold() {
    return enableModifiers(SGR.BOLD);
  }

  public TerminalPrinter stopBold() {
    return disableModifiers(SGR.BOLD);
  }

  public void endOfLine() {
    terminal.print(column, row, StringUtils.repeat(" ", terminalColumns - column));
    row += 1;
    column = 0;
  }
}
