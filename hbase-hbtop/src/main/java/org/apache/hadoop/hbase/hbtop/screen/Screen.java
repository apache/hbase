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
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.top.TopScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.impl.TerminalImpl;
import org.apache.hadoop.hbase.hbtop.terminal.impl.batch.BatchTerminal;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This dispatches key presses and timers to the current {@link ScreenView}.
 */
@InterfaceAudience.Private
public class Screen implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Screen.class);
  private static final long SLEEP_TIMEOUT_MILLISECONDS = 100;

  private final Connection connection;
  private final Admin admin;
  private final Terminal terminal;

  private ScreenView currentScreenView;
  private Long timerTimestamp;

  public Screen(Configuration conf, long initialRefreshDelay, Mode initialMode,
    @Nullable List<Field> initialFields, @Nullable Field initialSortField,
    @Nullable Boolean initialAscendingSort, @Nullable List<RecordFilter> initialFilters,
    long numberOfIterations, boolean batchMode)
    throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    admin = connection.getAdmin();

    // The first screen is the top screen
    if (batchMode) {
      terminal = new BatchTerminal();
    } else {
      terminal = new TerminalImpl("hbtop");
    }
    currentScreenView = new TopScreenView(this, terminal, initialRefreshDelay, admin,
      initialMode, initialFields, initialSortField, initialAscendingSort, initialFilters,
      numberOfIterations);
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
              TimeUnit.MILLISECONDS
                .sleep(Math.min(timerTimestamp - now, SLEEP_TIMEOUT_MILLISECONDS));
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
        LOGGER.error("Caught an exception", e);
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
