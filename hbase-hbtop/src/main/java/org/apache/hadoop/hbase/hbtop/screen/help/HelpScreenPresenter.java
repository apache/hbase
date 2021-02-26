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

import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The presentation logic for the help screen.
 */
@InterfaceAudience.Private
public class HelpScreenPresenter {

  private static final CommandDescription[] COMMAND_DESCRIPTIONS = new CommandDescription[] {
    new CommandDescription("f", "Add/Remove/Order/Sort the fields"),
    new CommandDescription("R", "Toggle the sort order (ascending/descending)"),
    new CommandDescription("m", "Select mode"),
    new CommandDescription("o", "Add a filter with ignoring case"),
    new CommandDescription("O", "Add a filter with case sensitive"),
    new CommandDescription("^o", "Show the current filters"),
    new CommandDescription("=", "Clear the current filters"),
    new CommandDescription("i", "Drill down"),
    new CommandDescription(
      Arrays.asList("up", "down", "left", "right", "pageUp", "pageDown", "home", "end"),
      "Scroll the metrics"),
    new CommandDescription("d", "Change the refresh delay"),
    new CommandDescription("X", "Adjust the field length"),
    new CommandDescription("<Enter>", "Refresh the display"),
    new CommandDescription("h", "Display this screen"),
    new CommandDescription(Arrays.asList("q", "<Esc>"), "Quit")
  };

  private final HelpScreenView helpScreenView;
  private final long refreshDelay;
  private final ScreenView nextScreenView;

  public HelpScreenPresenter(HelpScreenView helpScreenView, long refreshDelay,
    ScreenView nextScreenView) {
    this.helpScreenView = Objects.requireNonNull(helpScreenView);
    this.refreshDelay = refreshDelay;
    this.nextScreenView = Objects.requireNonNull(nextScreenView);
  }

  public void init() {
    helpScreenView.hideCursor();
    helpScreenView.clearTerminal();
    helpScreenView.showHelpScreen(refreshDelay, COMMAND_DESCRIPTIONS);
    helpScreenView.refreshTerminal();
  }

  public ScreenView transitionToNextScreen() {
    return nextScreenView;
  }
}
