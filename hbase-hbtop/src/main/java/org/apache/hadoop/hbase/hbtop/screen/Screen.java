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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.top.TopScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.impl.TerminalImpl;

/**
 * This dispatches key presses and timers to the current {@link ScreenView}.
 */
@InterfaceAudience.Private
public class Screen implements Closeable {

  private static final Log LOG = LogFactory.getLog(Screen.class);

  private static final long SLEEP_TIMEOUT_MILLISECONDS = 100;

  private final Connection connection;
  private final Admin admin;
  private final Terminal terminal;

  private ScreenView currentScreenView;
  private Long timerTimestamp;

  public Screen(Configuration conf, long initialRefreshDelay, Mode initialMode)
    throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    admin = connection.getAdmin();

    // The first screen is the top screen
    this.terminal = new TerminalImpl("hbtop");
    currentScreenView = new TopScreenView(this, terminal, initialRefreshDelay, admin,
      initialMode);
  }

  @Override
  public void close() throws IOException {
    try {
      admin.close();
    } finally {
      try {
        connection.close();
      } finally {
        terminal.close();
      }
    }
  }

  public void run() {
    currentScreenView.init();
    while (true) {
      try {
        KeyPress keyPress = terminal.pollKeyPress();

        ScreenView nextScreenView;
        if (keyPress != null) {
          // Dispatch the key press to the current screen
          nextScreenView = currentScreenView.handleKeyPress(keyPress);
        } else {
          if (timerTimestamp != null) {
            long now = System.currentTimeMillis();
            if (timerTimestamp <= now) {
              // Dispatch the timer to the current screen
              timerTimestamp = null;
              nextScreenView = currentScreenView.handleTimer();
            } else {
              if (timerTimestamp - now < SLEEP_TIMEOUT_MILLISECONDS) {
                TimeUnit.MILLISECONDS.sleep(timerTimestamp - now);
              } else {
                TimeUnit.MILLISECONDS.sleep(SLEEP_TIMEOUT_MILLISECONDS);
              }
              continue;
            }
          } else {
            TimeUnit.MILLISECONDS.sleep(SLEEP_TIMEOUT_MILLISECONDS);
            continue;
          }
        }

        // If the next screen is null, then exit
        if (nextScreenView == null) {
          return;
        }

        // If the next screen is not the previous, then go to the next screen
        if (nextScreenView != currentScreenView) {
          currentScreenView = nextScreenView;
          currentScreenView.init();
        }
      } catch (Exception e) {
        LOG.error("Caught an exception", e);
      }
    }
  }

  public void setTimer(long delay) {
    timerTimestamp = System.currentTimeMillis() + delay;
  }

  public void cancelTimer() {
    timerTimestamp = null;
  }
}
