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
 * The attributes of text in the terminal.
 */
@InterfaceAudience.Private
public class Attributes {
  private boolean bold;
  private boolean blink;
  private boolean reverse;
  private boolean underline;
  private Color foregroundColor;
  private Color backgroundColor;

  public Attributes() {
    reset();
  }

  public Attributes(Attributes attributes) {
    set(attributes);
  }

  public boolean isBold() {
    return bold;
  }

  public void setBold(boolean bold) {
    this.bold = bold;
  }

  public boolean isBlink() {
    return blink;
  }

  public void setBlink(boolean blink) {
    this.blink = blink;
  }

  public boolean isReverse() {
    return reverse;
  }

  public void setReverse(boolean reverse) {
    this.reverse = reverse;
  }

  public boolean isUnderline() {
    return underline;
  }

  public void setUnderline(boolean underline) {
    this.underline = underline;
  }

  public Color getForegroundColor() {
    return foregroundColor;
  }

  public void setForegroundColor(Color foregroundColor) {
    this.foregroundColor = foregroundColor;
  }

  public Color getBackgroundColor() {
    return backgroundColor;
  }

  public void setBackgroundColor(Color backgroundColor) {
    this.backgroundColor = backgroundColor;
  }

  public void reset() {
    bold = false;
    blink = false;
    reverse = false;
    underline = false;
    foregroundColor = Color.WHITE;
    backgroundColor = Color.BLACK;
  }

  public void set(Attributes attributes) {
    bold = attributes.bold;
    blink = attributes.blink;
    reverse = attributes.reverse;
    underline = attributes.underline;
    foregroundColor = attributes.foregroundColor;
    backgroundColor = attributes.backgroundColor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Attributes)) {
      return false;
    }
    Attributes that = (Attributes) o;
    return bold == that.bold && blink == that.blink && reverse == that.reverse
      && underline == that.underline && foregroundColor == that.foregroundColor
      && backgroundColor == that.backgroundColor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bold, blink, reverse, underline, foregroundColor, backgroundColor);
  }
}
