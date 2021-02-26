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
package org.apache.hadoop.hbase.hbtop.screen;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalSize;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * An abstract class for {@link ScreenView} that has the common useful methods and the default
 * implementations for the abstract methods.
 */
@InterfaceAudience.Private
public abstract class AbstractScreenView implements ScreenView {

  private final Screen screen;
  private final Terminal terminal;

  public AbstractScreenView(Screen screen, Terminal terminal) {
    this.screen = Objects.requireNonNull(screen);
    this.terminal = Objects.requireNonNull(terminal);
  }

  @Override
  public void init() {
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    return this;
  }

  @Override
  public ScreenView handleTimer() {
    return this;
  }

  protected Screen getScreen() {
    return screen;
  }

  protected Terminal getTerminal() {
    return terminal;
  }

  protected void setTimer(long delay) {
    screen.setTimer(delay);
  }

  protected void cancelTimer() {
    screen.cancelTimer();
  }

  protected TerminalPrinter getTerminalPrinter(int startRow) {
    return terminal.getTerminalPrinter(startRow);
  }

  @Nullable
  protected TerminalSize getTerminalSize() {
    return terminal.getSize();
  }

  @Nullable
  protected TerminalSize doResizeIfNecessary() {
    return terminal.doResizeIfNecessary();
  }

  public void clearTerminal() {
    terminal.clear();
  }

  public void refreshTerminal() {
    terminal.refresh();
  }

  public void hideCursor() {
    terminal.hideCursor();
  }

  public void setCursorPosition(int column, int row) {
    terminal.setCursorPosition(column, row);
  }
}
