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
package org.apache.hadoop.hbase.hbtop.terminal;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.hbtop.terminal.impl.TerminalImpl;


public final class CursorTest {

  private CursorTest() {
  }

  public static void main(String[] args) throws Exception {
    try (Terminal terminal = new TerminalImpl()) {
      terminal.refresh();
      terminal.setCursorPosition(0, 0);

      terminal.getTerminalPrinter(0).print("aaa").endOfLine();
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.getTerminalPrinter(0).print("bbb").endOfLine();
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.setCursorPosition(1, 0);
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.setCursorPosition(2, 0);
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.setCursorPosition(3, 0);
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.setCursorPosition(0, 1);
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.getTerminalPrinter(1).print("ccc").endOfLine();
      terminal.refresh();
      TimeUnit.SECONDS.sleep(1);

      terminal.getTerminalPrinter(3).print("Press any key to finish").endOfLine();
      terminal.refresh();

      while (true) {
        KeyPress keyPress = terminal.pollKeyPress();
        if (keyPress == null) {
          TimeUnit.MILLISECONDS.sleep(100);
          continue;
        }
        break;
      }
    }
  }
}
