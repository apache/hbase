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

import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The message mode in the top screen.
 */
@InterfaceAudience.Private
public class MessageModeScreenView extends AbstractScreenView {

  private final int row;
  private final MessageModeScreenPresenter messageModeScreenPresenter;

  public MessageModeScreenView(Screen screen, Terminal terminal, int row, String message,
    ScreenView nextScreenView) {
    super(screen, terminal);
    this.row = row;
    this.messageModeScreenPresenter =
      new MessageModeScreenPresenter(this, message, nextScreenView);
  }

  @Override
  public void init() {
    messageModeScreenPresenter.init();
    setTimer(2000);
  }

  @Override
  public ScreenView handleTimer() {
    return messageModeScreenPresenter.returnToNextScreen();
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    cancelTimer();
    return messageModeScreenPresenter.returnToNextScreen();
  }

  public void showMessage(String message) {
    getTerminalPrinter(row).startHighlight().print(" ").print(message).print(" ").stopHighlight()
      .endOfLine();
  }
}
