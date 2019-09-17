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
package org.apache.hadoop.hbase.hbtop.screen.mode;

import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The screen where we can choose the {@link Mode} in the top screen.
 */
@InterfaceAudience.Private
public class ModeScreenView extends AbstractScreenView {

  private static final int SCREEN_DESCRIPTION_START_ROW = 0;
  private static final int MODE_START_ROW = 4;

  private final ModeScreenPresenter modeScreenPresenter;

  public ModeScreenView(Screen screen, Terminal terminal, Mode currentMode,
    Consumer<Mode> resultListener, ScreenView nextScreenView) {
    super(screen, terminal);
    this.modeScreenPresenter = new ModeScreenPresenter(this, currentMode, resultListener,
      nextScreenView);
  }

  @Override
  public void init() {
    modeScreenPresenter.init();
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    switch (keyPress.getType()) {
      case Escape:
        return modeScreenPresenter.transitionToNextScreen(false);

      case Enter:
        return modeScreenPresenter.transitionToNextScreen(true);

      case ArrowUp:
        modeScreenPresenter.arrowUp();
        return this;

      case ArrowDown:
        modeScreenPresenter.arrowDown();
        return this;

      case PageUp:
      case Home:
        modeScreenPresenter.pageUp();
        return this;

      case PageDown:
      case End:
        modeScreenPresenter.pageDown();
        return this;

      default:
        // Do nothing
        break;
    }

    if (keyPress.getType() != KeyPress.Type.Character) {
      return this;
    }

    assert keyPress.getCharacter() != null;
    switch (keyPress.getCharacter()) {
      case 'q':
        return modeScreenPresenter.transitionToNextScreen(false);

      default:
        // Do nothing
        break;
    }

    return this;
  }

  public void showModeScreen(Mode currentMode, List<Mode> modes, int currentPosition,
    int modeHeaderMaxLength, int modeDescriptionMaxLength) {
    showScreenDescription(currentMode);

    for (int i = 0; i < modes.size(); i++) {
      showMode(i, modes.get(i), i == currentPosition,
        modeHeaderMaxLength, modeDescriptionMaxLength);
    }
  }

  private void showScreenDescription(Mode currentMode) {
    TerminalPrinter printer = getTerminalPrinter(SCREEN_DESCRIPTION_START_ROW);
    printer.startBold().print("Mode Management").stopBold().endOfLine();
    printer.print("Current mode: ")
      .startBold().print(currentMode.getHeader()).stopBold().endOfLine();
    printer.print("Select mode followed by <Enter>").endOfLine();
  }

  public void showMode(int pos, Mode mode, boolean selected, int modeHeaderMaxLength,
    int modeDescriptionMaxLength) {

    String modeHeader = String.format("%-" + modeHeaderMaxLength + "s", mode.getHeader());
    String modeDescription = String.format("%-" + modeDescriptionMaxLength + "s",
      mode.getDescription());

    int row = MODE_START_ROW + pos;
    TerminalPrinter printer = getTerminalPrinter(row);
    if (selected) {
      printer.startHighlight().print(modeHeader).stopHighlight()
        .printFormat(" = %s", modeDescription).endOfLine();
    } else {
      printer.printFormat("%s = %s", modeHeader, modeDescription).endOfLine();
    }
  }
}
