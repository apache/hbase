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
package org.apache.hadoop.hbase.hbtop.terminal.impl;

import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.clearAll;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.cursor;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.moveCursor;
import static org.apache.hadoop.hbase.hbtop.terminal.impl.EscapeSequences.normal;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.hbtop.terminal.CursorPosition;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The implementation of the {@link Terminal} interface.
 */
@InterfaceAudience.Private
public class TerminalImpl implements Terminal {

  private static final Logger LOGGER = LoggerFactory.getLogger(TerminalImpl.class);

  private TerminalSize cachedTerminalSize;

  private final PrintWriter output;

  private final ScreenBuffer screenBuffer;

  private final Queue<KeyPress> keyPressQueue;
  private final KeyPressGenerator keyPressGenerator;

  public TerminalImpl() {
    this(null);
  }

  public TerminalImpl(@Nullable String title) {
    output = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
    sttyRaw();

    if (title != null) {
      setTitle(title);
    }

    screenBuffer = new ScreenBuffer();

    cachedTerminalSize = queryTerminalSize();
    updateTerminalSize(cachedTerminalSize.getColumns(), cachedTerminalSize.getRows());

    keyPressQueue = new ConcurrentLinkedQueue<>();
    keyPressGenerator = new KeyPressGenerator(System.in, keyPressQueue);
    keyPressGenerator.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      output.printf("%s%s%s%s", moveCursor(0, 0), cursor(true), normal(), clearAll());
      output.flush();
      sttyCooked();
    }));

    // Clear the terminal
    output.write(clearAll());
    output.flush();
  }

  private void setTitle(String title) {
    output.write(EscapeSequences.setTitle(title));
    output.flush();
  }

  private void updateTerminalSize(int columns, int rows) {
    screenBuffer.reallocate(columns, rows);
  }

  @Override
  public void clear() {
    screenBuffer.clear();
  }

  @Override
  public void refresh() {
    screenBuffer.flush(output);
  }

  @Override
  public TerminalSize getSize() {
    return cachedTerminalSize;
  }

  @Nullable
  @Override
  public TerminalSize doResizeIfNecessary() {
    TerminalSize currentTerminalSize = queryTerminalSize();
    if (!currentTerminalSize.equals(cachedTerminalSize)) {
      cachedTerminalSize = currentTerminalSize;
      updateTerminalSize(cachedTerminalSize.getColumns(), cachedTerminalSize.getRows());
      return cachedTerminalSize;
    }
    return null;
  }

  @Nullable
  @Override
  public KeyPress pollKeyPress() {
    return keyPressQueue.poll();
  }

  @Override
  public CursorPosition getCursorPosition() {
    return screenBuffer.getCursorPosition();
  }

  @Override
  public void setCursorPosition(int column, int row) {
    screenBuffer.setCursorPosition(column, row);
  }

  @Override
  public void hideCursor() {
    screenBuffer.hideCursor();
  }

  @Override
  public TerminalPrinter getTerminalPrinter(int startRow) {
    return new TerminalPrinterImpl(screenBuffer, startRow);
  }

  @Override
  public void close() {
    keyPressGenerator.stop();
  }

  private TerminalSize queryTerminalSize() {
    String sizeString = doStty("size");

    int rows = 0;
    int columns = 0;

    StringTokenizer tokenizer = new StringTokenizer(sizeString);
    int rc = Integer.parseInt(tokenizer.nextToken());
    if (rc > 0) {
      rows = rc;
    }

    rc = Integer.parseInt(tokenizer.nextToken());
    if (rc > 0) {
      columns = rc;
    }
    return new TerminalSize(columns, rows);
  }

  private void sttyRaw() {
    doStty("-ignbrk -brkint -parmrk -istrip -inlcr -igncr -icrnl -ixon -opost " +
      "-echo -echonl -icanon -isig -iexten -parenb cs8 min 1");
  }

  private void sttyCooked() {
    doStty("sane cooked");
  }

  private String doStty(String sttyOptionsString) {
    String [] cmd = {"/bin/sh", "-c", "stty " + sttyOptionsString + " < /dev/tty"};

    try {
      Process process = Runtime.getRuntime().exec(cmd);

      String ret;

      // stdout
      try (BufferedReader stdout = new BufferedReader(new InputStreamReader(
        process.getInputStream(), StandardCharsets.UTF_8))) {
        ret = stdout.readLine();
      }

      // stderr
      try (BufferedReader stderr = new BufferedReader(new InputStreamReader(
        process.getErrorStream(), StandardCharsets.UTF_8))) {
        String line = stderr.readLine();
        if ((line != null) && (line.length() > 0)) {
          LOGGER.error("Error output from stty: " + line);
        }
      }

      try {
        process.waitFor();
      } catch (InterruptedException ignored) {
      }

      int exitValue = process.exitValue();
      if (exitValue != 0) {
        LOGGER.error("stty returned error code: " + exitValue);
      }
      return ret;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
