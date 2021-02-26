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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * This generates {@link KeyPress} objects from the given input stream and offers them to the
 * given queue.
 */
@InterfaceAudience.Private
public class KeyPressGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyPressGenerator.class);

  private enum ParseState {
    START, ESCAPE, ESCAPE_SEQUENCE_PARAM1, ESCAPE_SEQUENCE_PARAM2
  }

  private final Queue<KeyPress> keyPressQueue;
  private final BlockingQueue<Character> inputCharacterQueue = new LinkedBlockingQueue<>();
  private final Reader input;
  private final InputStream inputStream;
  private final AtomicBoolean stopThreads = new AtomicBoolean();
  private final ExecutorService executorService;

  private ParseState parseState;
  private int param1;
  private int param2;

  public KeyPressGenerator(InputStream inputStream, Queue<KeyPress> keyPressQueue) {
    this.inputStream = inputStream;
    input = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    this.keyPressQueue = keyPressQueue;

    executorService = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder()
      .setNameFormat("KeyPressGenerator-%d").setDaemon(true)
      .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());

    initState();
  }

  public void start() {
    executorService.execute(this::readerThread);
    executorService.execute(this::generatorThread);
  }

  private void initState() {
    parseState = ParseState.START;
    param1 = 0;
    param2 = 0;
  }

  private void readerThread() {
    boolean done = false;
    char[] readBuffer = new char[128];

    while (!done && !stopThreads.get()) {
      try {
        int n = inputStream.available();
        if (n > 0) {
          if (readBuffer.length < n) {
            readBuffer = new char[readBuffer.length * 2];
          }

          int rc = input.read(readBuffer, 0, readBuffer.length);
          if (rc == -1) {
            // EOF
            done = true;
          } else {
            for (int i = 0; i < rc; i++) {
              int ch = readBuffer[i];
              inputCharacterQueue.offer((char) ch);
            }
          }
        } else {
          Thread.sleep(20);
        }
      } catch (InterruptedException ignored) {
      } catch (IOException e) {
        LOGGER.error("Caught an exception", e);
        done = true;
      }
    }
  }

  private void generatorThread() {
    while (!stopThreads.get()) {
      Character ch;
      try {
        ch = inputCharacterQueue.poll(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
        continue;
      }

      if (ch == null) {
        if (parseState == ParseState.ESCAPE) {
          offer(new KeyPress(KeyPress.Type.Escape, null, false, false, false));
          initState();
        } else if (parseState != ParseState.START) {
          offer(new KeyPress(KeyPress.Type.Unknown, null, false, false, false));
          initState();
        }
        continue;
      }

      if (parseState == ParseState.START) {
        if (ch == 0x1B) {
          parseState = ParseState.ESCAPE;
          continue;
        }

        switch (ch) {
          case '\n':
          case '\r':
            offer(new KeyPress(KeyPress.Type.Enter, '\n', false, false, false));
            continue;

          case 0x08:
          case 0x7F:
            offer(new KeyPress(KeyPress.Type.Backspace, '\b', false, false, false));
            continue;

          case '\t':
            offer(new KeyPress(KeyPress.Type.Tab, '\t', false, false, false));
            continue;

          default:
            // Do nothing
            break;
        }

        if (ch < 32) {
          ctrlAndCharacter(ch);
          continue;
        }

        if (isPrintableChar(ch)) {
          // Normal character
          offer(new KeyPress(KeyPress.Type.Character, ch, false, false, false));
          continue;
        }

        offer(new KeyPress(KeyPress.Type.Unknown, null, false, false, false));
        continue;
      }

      if (parseState == ParseState.ESCAPE) {
        if (ch == 0x1B) {
          offer(new KeyPress(KeyPress.Type.Escape, null, false, false, false));
          continue;
        }

        if (ch < 32 && ch != 0x08) {
          ctrlAltAndCharacter(ch);
          initState();
          continue;
        } else if (ch == 0x7F || ch == 0x08) {
          offer(new KeyPress(KeyPress.Type.Backspace, '\b', false, false, false));
          initState();
          continue;
        }

        if (ch == '[' || ch == 'O') {
          parseState = ParseState.ESCAPE_SEQUENCE_PARAM1;
          continue;
        }

        if (isPrintableChar(ch)) {
          // Alt and character
          offer(new KeyPress(KeyPress.Type.Character, ch, true, false, false));
          initState();
          continue;
        }

        offer(new KeyPress(KeyPress.Type.Escape, null, false, false, false));
        offer(new KeyPress(KeyPress.Type.Unknown, null, false, false, false));
        initState();
        continue;
      }

      escapeSequenceCharacter(ch);
    }
  }

  private void ctrlAndCharacter(char ch) {
    char ctrlCode;
    switch (ch) {
      case 0:
        ctrlCode = ' ';
        break;

      case 28:
        ctrlCode = '\\';
        break;

      case 29:
        ctrlCode = ']';
        break;

      case 30:
        ctrlCode = '^';
        break;

      case 31:
        ctrlCode = '_';
        break;

      default:
        ctrlCode = (char) ('a' - 1 + ch);
        break;
    }
    offer(new KeyPress(KeyPress.Type.Character, ctrlCode, false, true, false));
  }

  private boolean isPrintableChar(char ch) {
    if (Character.isISOControl(ch)) {
      return false;
    }
    Character.UnicodeBlock block = Character.UnicodeBlock.of(ch);
    return block != null && !block.equals(Character.UnicodeBlock.SPECIALS);
  }

  private void ctrlAltAndCharacter(char ch) {
    char ctrlCode;
    switch (ch) {
      case 0:
        ctrlCode = ' ';
        break;

      case 28:
        ctrlCode = '\\';
        break;

      case 29:
        ctrlCode = ']';
        break;

      case 30:
        ctrlCode = '^';
        break;

      case 31:
        ctrlCode = '_';
        break;

      default:
        ctrlCode = (char) ('a' - 1 + ch);
        break;
    }
    offer(new KeyPress(KeyPress.Type.Character, ctrlCode, true, true, false));
  }

  private void escapeSequenceCharacter(char ch) {
    switch (parseState) {
      case ESCAPE_SEQUENCE_PARAM1:
        if (ch == ';') {
          parseState = ParseState.ESCAPE_SEQUENCE_PARAM2;
        } else if (Character.isDigit(ch)) {
          param1 = param1 * 10 + Character.digit(ch, 10);
        } else {
          doneEscapeSequenceCharacter(ch);
        }
        break;

      case ESCAPE_SEQUENCE_PARAM2:
        if (Character.isDigit(ch)) {
          param2 = param2 * 10 + Character.digit(ch, 10);
        } else {
          doneEscapeSequenceCharacter(ch);
        }
        break;

      default:
        throw new AssertionError();
    }
  }

  private void doneEscapeSequenceCharacter(char last) {
    boolean alt = false;
    boolean ctrl = false;
    boolean shift = false;
    if (param2 != 0) {
      alt = isAlt(param2);
      ctrl = isCtrl(param2);
      shift = isShift(param2);
    }

    if (last != '~') {
      switch (last) {
        case 'A':
          offer(new KeyPress(KeyPress.Type.ArrowUp, null, alt, ctrl, shift));
          break;

        case 'B':
          offer(new KeyPress(KeyPress.Type.ArrowDown, null, alt, ctrl, shift));
          break;

        case 'C':
          offer(new KeyPress(KeyPress.Type.ArrowRight, null, alt, ctrl, shift));
          break;

        case 'D':
          offer(new KeyPress(KeyPress.Type.ArrowLeft, null, alt, ctrl, shift));
          break;

        case 'H':
          offer(new KeyPress(KeyPress.Type.Home, null, alt, ctrl, shift));
          break;

        case 'F':
          offer(new KeyPress(KeyPress.Type.End, null, alt, ctrl, shift));
          break;

        case 'P':
          offer(new KeyPress(KeyPress.Type.F1, null, alt, ctrl, shift));
          break;

        case 'Q':
          offer(new KeyPress(KeyPress.Type.F2, null, alt, ctrl, shift));
          break;

        case 'R':
          offer(new KeyPress(KeyPress.Type.F3, null, alt, ctrl, shift));
          break;

        case 'S':
          offer(new KeyPress(KeyPress.Type.F4, null, alt, ctrl, shift));
          break;

        case 'Z':
          offer(new KeyPress(KeyPress.Type.ReverseTab, null, alt, ctrl, shift));
          break;

        default:
          offer(new KeyPress(KeyPress.Type.Unknown, null, alt, ctrl, shift));
          break;
      }
      initState();
      return;
    }

    switch (param1) {
      case 1:
        offer(new KeyPress(KeyPress.Type.Home, null, alt, ctrl, shift));
        break;

      case 2:
        offer(new KeyPress(KeyPress.Type.Insert, null, alt, ctrl, shift));
        break;

      case 3:
        offer(new KeyPress(KeyPress.Type.Delete, null, alt, ctrl, shift));
        break;

      case 4:
        offer(new KeyPress(KeyPress.Type.End, null, alt, ctrl, shift));
        break;

      case 5:
        offer(new KeyPress(KeyPress.Type.PageUp, null, alt, ctrl, shift));
        break;

      case 6:
        offer(new KeyPress(KeyPress.Type.PageDown, null, alt, ctrl, shift));
        break;

      case 11:
        offer(new KeyPress(KeyPress.Type.F1, null, alt, ctrl, shift));
        break;

      case 12:
        offer(new KeyPress(KeyPress.Type.F2, null, alt, ctrl, shift));
        break;

      case 13:
        offer(new KeyPress(KeyPress.Type.F3, null, alt, ctrl, shift));
        break;

      case 14:
        offer(new KeyPress(KeyPress.Type.F4, null, alt, ctrl, shift));
        break;

      case 15:
        offer(new KeyPress(KeyPress.Type.F5, null, alt, ctrl, shift));
        break;

      case 17:
        offer(new KeyPress(KeyPress.Type.F6, null, alt, ctrl, shift));
        break;

      case 18:
        offer(new KeyPress(KeyPress.Type.F7, null, alt, ctrl, shift));
        break;

      case 19:
        offer(new KeyPress(KeyPress.Type.F8, null, alt, ctrl, shift));
        break;

      case 20:
        offer(new KeyPress(KeyPress.Type.F9, null, alt, ctrl, shift));
        break;

      case 21:
        offer(new KeyPress(KeyPress.Type.F10, null, alt, ctrl, shift));
        break;

      case 23:
        offer(new KeyPress(KeyPress.Type.F11, null, alt, ctrl, shift));
        break;

      case 24:
        offer(new KeyPress(KeyPress.Type.F12, null, alt, ctrl, shift));
        break;

      default:
        offer(new KeyPress(KeyPress.Type.Unknown, null, false, false, false));
        break;
    }

    initState();
  }

  private boolean isShift(int param) {
    return (param & 1) != 0;
  }

  private boolean isAlt(int param) {
    return (param & 2) != 0;
  }

  private boolean isCtrl(int param) {
    return (param & 4) != 0;
  }

  private void offer(KeyPress keyPress) {
    // Handle ctrl + c
    if (keyPress.isCtrl() && keyPress.getType() == KeyPress.Type.Character &&
      keyPress.getCharacter() == 'c') {
      System.exit(0);
    }

    keyPressQueue.offer(keyPress);
  }

  public void stop() {
    stopThreads.set(true);

    executorService.shutdown();
    try {
      while (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        LOGGER.warn("Waiting for thread-pool to terminate");
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted while waiting for thread-pool termination", e);
    }
  }
}
