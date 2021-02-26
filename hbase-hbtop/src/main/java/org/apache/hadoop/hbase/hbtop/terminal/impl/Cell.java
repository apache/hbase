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
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a single text cell of the terminal.
 */
@InterfaceAudience.Private
public class Cell {
  private static final char UNSET_VALUE = (char) 65535;
  private static final char END_OF_LINE = '\0';

  private final Attributes attributes;
  private char ch;

  public Cell() {
    attributes = new Attributes();
    ch = ' ';
  }

  public char getChar() {
    return ch;
  }

  public void setChar(char ch) {
    this.ch = ch;
  }

  public void reset() {
    attributes.reset();
    ch = ' ';
  }

  public void unset() {
    attributes.reset();
    ch = UNSET_VALUE;
  }

  public void endOfLine() {
    attributes.reset();
    ch = END_OF_LINE;
  }

  public boolean isEndOfLine() {
    return ch == END_OF_LINE;
  }

  public void set(Cell cell) {
    attributes.set(cell.attributes);
    this.ch = cell.ch;
  }

  public Attributes getAttributes() {
    return new Attributes(attributes);
  }

  public void setAttributes(Attributes attributes) {
    this.attributes.set(attributes);
  }

  public boolean isBold() {
    return attributes.isBold();
  }

  public boolean isBlink() {
    return attributes.isBlink();
  }

  public boolean isReverse() {
    return attributes.isReverse();
  }

  public boolean isUnderline() {
    return attributes.isUnderline();
  }

  public Color getForegroundColor() {
    return attributes.getForegroundColor();
  }

  public Color getBackgroundColor() {
    return attributes.getBackgroundColor();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Cell)) {
      return false;
    }
    Cell cell = (Cell) o;
    return ch == cell.ch && attributes.equals(cell.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes, ch);
  }
}
