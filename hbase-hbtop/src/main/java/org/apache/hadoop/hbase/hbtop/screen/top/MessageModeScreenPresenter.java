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

import java.util.Objects;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The presentation logic for the message mode.
 *
 * Exit after 2 seconds or if any key is pressed.
 */
@InterfaceAudience.Private
public class MessageModeScreenPresenter {

  private final MessageModeScreenView messageModeScreenView;
  private final String message;
  private final ScreenView nextScreenView;

  public MessageModeScreenPresenter(MessageModeScreenView messageModeScreenView, String message,
    ScreenView nextScreenView) {
    this.messageModeScreenView = Objects.requireNonNull(messageModeScreenView);
    this.message = Objects.requireNonNull(message);
    this.nextScreenView = Objects.requireNonNull(nextScreenView);
  }

  public void init() {
    messageModeScreenView.showMessage(message);
    messageModeScreenView.refreshTerminal();
  }

  public ScreenView returnToNextScreen() {
    return nextScreenView;
  }
}
