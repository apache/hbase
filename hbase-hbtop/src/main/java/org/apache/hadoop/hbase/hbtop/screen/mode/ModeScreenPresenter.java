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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The presentation logic for the mode screen.
 */
@InterfaceAudience.Private
public class ModeScreenPresenter {

  private final ModeScreenView modeScreenView;
  private final Mode currentMode;
  private final Consumer<Mode> resultListener;
  private final ScreenView nextScreenView;

  private final int modeHeaderMaxLength;
  private final int modeDescriptionMaxLength;
  private final List<Mode> modes = Arrays.asList(Mode.values());

  private int currentPosition;

  public ModeScreenPresenter(ModeScreenView modeScreenView, Mode currentMode,
    Consumer<Mode> resultListener, ScreenView nextScreenView) {
    this.modeScreenView = Objects.requireNonNull(modeScreenView);
    this.currentMode = Objects.requireNonNull(currentMode);
    this.resultListener = Objects.requireNonNull(resultListener);
    this.nextScreenView = Objects.requireNonNull(nextScreenView);

    int modeHeaderLength = 0;
    int modeDescriptionLength = 0;
    for (int i = 0; i < modes.size(); i++) {
      Mode mode = modes.get(i);
      if (mode == currentMode) {
        currentPosition = i;
      }

      if (modeHeaderLength < mode.getHeader().length()) {
        modeHeaderLength = mode.getHeader().length();
      }

      if (modeDescriptionLength < mode.getDescription().length()) {
        modeDescriptionLength = mode.getDescription().length();
      }
    }

    modeHeaderMaxLength = modeHeaderLength;
    modeDescriptionMaxLength = modeDescriptionLength;
  }

  public void init() {
    modeScreenView.hideCursor();
    modeScreenView.clearTerminal();
    modeScreenView.showModeScreen(currentMode, modes, currentPosition, modeHeaderMaxLength,
      modeDescriptionMaxLength);
    modeScreenView.refreshTerminal();
  }

  public void arrowUp() {
    if (currentPosition > 0) {
      currentPosition -= 1;
      showMode(currentPosition);
      showMode(currentPosition + 1);
      modeScreenView.refreshTerminal();
    }
  }

  public void arrowDown() {
    if (currentPosition < modes.size() - 1) {
      currentPosition += 1;
      showMode(currentPosition);
      showMode(currentPosition - 1);
      modeScreenView.refreshTerminal();
    }
  }

  public void pageUp() {
    if (currentPosition > 0) {
      int previousPosition = currentPosition;
      currentPosition = 0;
      showMode(previousPosition);
      showMode(currentPosition);
      modeScreenView.refreshTerminal();
    }
  }

  public void pageDown() {
    if (currentPosition < modes.size() - 1) {
      int previousPosition = currentPosition;
      currentPosition = modes.size() - 1;
      showMode(previousPosition);
      showMode(currentPosition);
      modeScreenView.refreshTerminal();
    }
  }

  private void showMode(int pos) {
    modeScreenView.showMode(pos, modes.get(pos), pos == currentPosition, modeHeaderMaxLength,
      modeDescriptionMaxLength);
  }

  public ScreenView transitionToNextScreen(boolean changeMode) {
    Mode selectedMode = modes.get(currentPosition);
    if (changeMode && currentMode != selectedMode) {
      resultListener.accept(selectedMode);
    }
    return nextScreenView;
  }
}
