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
package org.apache.hadoop.hbase.hbtop.screen.help;

import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The help screen.
 */
@InterfaceAudience.Private
public class HelpScreenView extends AbstractScreenView {

  private static final int SCREEN_DESCRIPTION_START_ROW = 0;
  private static final int COMMAND_DESCRIPTION_START_ROW = 3;

  private final HelpScreenPresenter helpScreenPresenter;

  public HelpScreenView(Screen screen, Terminal terminal, long refreshDelay,
    ScreenView nextScreenView) {
    super(screen, terminal);
    this.helpScreenPresenter = new HelpScreenPresenter(this, refreshDelay, nextScreenView);
  }

  @Override
  public void init() {
    helpScreenPresenter.init();
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    return helpScreenPresenter.transitionToNextScreen();
  }

  public void showHelpScreen(long refreshDelay, CommandDescription[] commandDescriptions) {
    showScreenDescription(refreshDelay);

    TerminalPrinter printer = getTerminalPrinter(COMMAND_DESCRIPTION_START_ROW);
    for (CommandDescription commandDescription : commandDescriptions) {
      showCommandDescription(printer, commandDescription);
    }

    printer.endOfLine();
    printer.print("Press any key to continue").endOfLine();
  }

  private void showScreenDescription(long refreshDelay) {
    TerminalPrinter printer = getTerminalPrinter(SCREEN_DESCRIPTION_START_ROW);
    printer.startBold().print("Help for Interactive Commands").stopBold().endOfLine();
    printer.print("Refresh delay: ").startBold()
      .print((double) refreshDelay / 1000).stopBold().endOfLine();
  }

  private void showCommandDescription(TerminalPrinter terminalPrinter,
    CommandDescription commandDescription) {
    terminalPrinter.print("  ");
    boolean first = true;
    for (String key : commandDescription.getKeys()) {
      if (first) {
        first = false;
      } else {
        terminalPrinter.print(",");
      }
      terminalPrinter.startBold().print(key).stopBold();
    }

    terminalPrinter.printFormat(": %s", commandDescription.getDescription()).endOfLine();
  }
}
