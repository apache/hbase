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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The presentation logic for the input mode.
 */
@InterfaceAudience.Private
public class InputModeScreenPresenter {
  private final InputModeScreenView inputModeScreenView;
  private final String message;
  private final List<String> histories;
  private final Function<String, ScreenView> resultListener;

  private StringBuilder inputString = new StringBuilder();
  private int cursorPosition;
  private int historyPosition = -1;

  public InputModeScreenPresenter(InputModeScreenView inputModeScreenView, String message,
    @Nullable List<String> histories, Function<String, ScreenView> resultListener) {
    this.inputModeScreenView = Objects.requireNonNull(inputModeScreenView);
    this.message = Objects.requireNonNull(message);

    if (histories != null) {
      this.histories = Collections.unmodifiableList(new ArrayList<>(histories));
    } else {
      this.histories = Collections.emptyList();
    }

    this.resultListener = Objects.requireNonNull(resultListener);
  }

  public void init() {
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public ScreenView returnToNextScreen() {
    inputModeScreenView.hideCursor();
    String result = inputString.toString();

    return resultListener.apply(result);
  }

  public void character(Character character) {
    inputString.insert(cursorPosition, character);
    cursorPosition += 1;
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void backspace() {
    if (cursorPosition == 0) {
      return;
    }

    inputString.deleteCharAt(cursorPosition - 1);
    cursorPosition -= 1;
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void delete() {
    if (inputString.length() == 0 || cursorPosition > inputString.length() - 1) {
      return;
    }

    inputString.deleteCharAt(cursorPosition);
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void arrowLeft() {
    if (cursorPosition == 0) {
      return;
    }

    cursorPosition -= 1;
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void arrowRight() {
    if (cursorPosition > inputString.length() - 1) {
      return;
    }

    cursorPosition += 1;
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void home() {
    cursorPosition = 0;
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void end() {
    cursorPosition = inputString.length();
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void arrowUp() {
    if (historyPosition == 0 || histories.isEmpty()) {
      return;
    }

    if (historyPosition == -1) {
      historyPosition = histories.size() - 1;
    } else {
      historyPosition -= 1;
    }

    inputString = new StringBuilder(histories.get(historyPosition));

    cursorPosition = inputString.length();
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }

  public void arrowDown() {
    if (historyPosition == -1 || histories.isEmpty()) {
      return;
    }

    if (historyPosition == histories.size() - 1) {
      historyPosition = -1;
      inputString = new StringBuilder();
    } else {
      historyPosition += 1;
      inputString = new StringBuilder(histories.get(historyPosition));
    }

    cursorPosition = inputString.length();
    inputModeScreenView.showInput(message, inputString.toString(), cursorPosition);
    inputModeScreenView.refreshTerminal();
  }
}
