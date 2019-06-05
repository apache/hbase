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

import com.googlecode.lanterna.TerminalPosition;
import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.input.KeyType;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.hbtop.terminal.CursorPosition;
import org.apache.hadoop.hbase.hbtop.terminal.KeyEvent;
import org.apache.hadoop.hbase.hbtop.terminal.SGR;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalSize;
import org.apache.hadoop.hbase.hbtop.terminal.TextColor;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Implementation of {@link Terminal} using Lanterna library.
 */
@InterfaceAudience.Private
public class LanternaTerminal implements Terminal {

  private final com.googlecode.lanterna.screen.Screen screen;
  private final TextGraphics textGraphics;

  public LanternaTerminal() {
    try {
      DefaultTerminalFactory defaultTerminalFactory = new DefaultTerminalFactory();
      com.googlecode.lanterna.terminal.Terminal terminal = defaultTerminalFactory.createTerminal();
      screen = new TerminalScreen(terminal);
      screen.startScreen();
      textGraphics = screen.newTextGraphics();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      screen.stopScreen();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void clear() {
    screen.clear();
  }

  @Override
  public void refresh() {
    try {
      screen.refresh();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public TerminalSize getSize() {
    com.googlecode.lanterna.TerminalSize terminalSize = screen.getTerminalSize();
    return new TerminalSize(terminalSize.getColumns(), terminalSize.getRows());
  }

  @Nullable
  @Override
  public TerminalSize doResizeIfNecessary() {
    com.googlecode.lanterna.TerminalSize terminalSize = screen.doResizeIfNecessary();
    return terminalSize == null ? null :
      new TerminalSize(terminalSize.getColumns(), terminalSize.getRows());
  }

  @Override
  public KeyEvent readKeyEvent() {
    KeyStroke keyStroke;
    try {
      keyStroke = screen.readInput();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return convertToKeyEvent(keyStroke);
  }

  @Nullable
  @Override
  public KeyEvent pollKeyEvent() {
    KeyStroke keyStroke;
    try {
      keyStroke = screen.pollInput();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (keyStroke == null) {
      return null;
    }

    return convertToKeyEvent(keyStroke);
  }

  private KeyEvent convertToKeyEvent(KeyStroke keyStroke) {
    KeyEvent.Type type;
    switch (keyStroke.getKeyType()) {
      case Character:
      case Escape:
      case Backspace:
      case ArrowLeft:
      case ArrowRight:
      case ArrowUp:
      case ArrowDown:
      case Insert:
      case Delete:
      case Home:
      case End:
      case PageUp:
      case PageDown:
      case Tab:
      case ReverseTab:
      case Enter:
      case F1:
      case F2:
      case F3:
      case F4:
      case F5:
      case F6:
      case F7:
      case F8:
      case F9:
      case F10:
      case F11:
      case F12:
      case F13:
      case F14:
      case F15:
      case F16:
      case F17:
      case F18:
      case F19:
        type = KeyEvent.Type.valueOf(keyStroke.getKeyType().toString());
        break;

      case EOF:
        throw new UncheckedIOException(new IOException("EOF"));

      default:
        type = KeyEvent.Type.Unknown;
        break;
    }

    Character character = null;
    if (keyStroke.getKeyType() == KeyType.Character) {
      character = keyStroke.getCharacter();
    }

    return new KeyEvent(type, character, keyStroke.isCtrlDown(), keyStroke.isAltDown(),
      keyStroke.isShiftDown(), keyStroke.getEventTime());
  }

  @Override
  public CursorPosition getCursorPosition() {
    TerminalPosition terminalPosition = screen.getCursorPosition();
    return new CursorPosition(terminalPosition.getColumn(), terminalPosition.getRow());
  }

  @Override
  public void setCursorPosition(@Nullable CursorPosition cursorPosition) {
    screen.setCursorPosition(cursorPosition == null ? null :
      new TerminalPosition(cursorPosition.getColumn(), cursorPosition.getRow()));
  }

  @Override
  public Terminal print(int column, int row, String value) {
    textGraphics.putString(column, row, value);
    return this;
  }

  @Override
  public Terminal setForegroundColor(TextColor textColor) {
    textGraphics.setForegroundColor(convertTextColor(textColor));
    return this;
  }

  @Override
  public Terminal setBackgroundColor(TextColor textColor) {
    textGraphics.setBackgroundColor(convertTextColor(textColor));
    return this;
  }

  private com.googlecode.lanterna.TextColor.ANSI convertTextColor(TextColor textColor) {
    return com.googlecode.lanterna.TextColor.ANSI.valueOf(textColor.name());
  }

  @Override
  public Terminal enableModifiers(SGR... modifiers) {
    textGraphics.enableModifiers(convertModifiers(modifiers));
    return null;
  }

  @Override
  public Terminal disableModifiers(SGR... modifiers) {
    textGraphics.disableModifiers(convertModifiers(modifiers));
    return null;
  }

  private com.googlecode.lanterna.SGR[] convertModifiers(SGR[] modifiers) {
    return Stream.of(modifiers)
      .map(m -> com.googlecode.lanterna.SGR.valueOf(m.name()))
      .collect(Collectors.toList())
      .toArray(new com.googlecode.lanterna.SGR[modifiers.length]);
  }
}
