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
package org.apache.hadoop.hbase.hbtop.screen.top;

import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The input mode in the top screen.
 */
@InterfaceAudience.Private
public class InputModeScreenView extends AbstractScreenView {

  private final int row;
  private final InputModeScreenPresenter inputModeScreenPresenter;

  public InputModeScreenView(Screen screen, Terminal terminal, int row, String message,
    List<String> histories, Function<String, ScreenView> resultListener) {
    super(screen, terminal);
    this.row = row;
    this.inputModeScreenPresenter = new InputModeScreenPresenter(this, message, histories,
      resultListener);
  }

  @Override
  public void init() {
    inputModeScreenPresenter.init();
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {

    switch (keyPress.getType()) {
      case Enter:
        return inputModeScreenPresenter.returnToNextScreen();

      case Character:
        inputModeScreenPresenter.character(keyPress.getCharacter());
        break;

      case Backspace:
        inputModeScreenPresenter.backspace();
        break;

      case Delete:
        inputModeScreenPresenter.delete();
        break;

      case ArrowLeft:
        inputModeScreenPresenter.arrowLeft();
        break;

      case ArrowRight:
        inputModeScreenPresenter.arrowRight();
        break;

      case Home:
        inputModeScreenPresenter.home();
        break;

      case End:
        inputModeScreenPresenter.end();
        break;

      case ArrowUp:
        inputModeScreenPresenter.arrowUp();
        break;

      case ArrowDown:
        inputModeScreenPresenter.arrowDown();
        break;

      default:
        break;
    }
    return this;
  }

  public void showInput(String message, String inputString, int cursorPosition) {
    getTerminalPrinter(row).startBold().print(message).stopBold().print(" ").print(inputString)
      .endOfLine();
    setCursorPosition(message.length() + 1 + cursorPosition, row);
    refreshTerminal();
  }
}
