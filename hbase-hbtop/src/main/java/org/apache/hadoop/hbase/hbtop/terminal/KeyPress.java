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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents the user pressing a key on the keyboard.
 */
@InterfaceAudience.Private
public class KeyPress {
  public enum Type {
    Character,
    Escape,
    Backspace,
    ArrowLeft,
    ArrowRight,
    ArrowUp,
    ArrowDown,
    Insert,
    Delete,
    Home,
    End,
    PageUp,
    PageDown,
    ReverseTab,
    Tab,
    Enter,
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
    Unknown
  }

  private final Type type;
  private final Character character;
  private final boolean alt;
  private final boolean ctrl;
  private final boolean shift;

  public KeyPress(Type type, @Nullable Character character, boolean alt, boolean ctrl,
    boolean shift) {
    this.type = Objects.requireNonNull(type);
    this.character = character;
    this.alt = alt;
    this.ctrl = ctrl;
    this.shift = shift;
  }

  public Type getType() {
    return type;
  }

  @Nullable
  public Character getCharacter() {
    return character;
  }

  public boolean isAlt() {
    return alt;
  }

  public boolean isCtrl() {
    return ctrl;
  }

  public boolean isShift() {
    return shift;
  }

  @Override
  public String toString() {
    return "KeyPress{" +
      "type=" + type +
      ", character=" + escape(character) +
      ", alt=" + alt +
      ", ctrl=" + ctrl +
      ", shift=" + shift +
      '}';
  }

  private String escape(Character character) {
    if (character == null) {
      return "null";
    }

    switch (character) {
      case '\n':
        return "\\n";

      case '\b':
        return "\\b";

      case '\t':
        return "\\t";

      default:
        return character.toString();
    }
  }
}
