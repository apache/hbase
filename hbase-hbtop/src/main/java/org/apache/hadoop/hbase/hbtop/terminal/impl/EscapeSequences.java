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

import org.apache.hadoop.hbase.hbtop.terminal.Color;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Utility class for escape sequences.
 */
@InterfaceAudience.Private
public final class EscapeSequences {

  private EscapeSequences() {
  }

  public static String clearAll() {
    return "\033[0;37;40m\033[2J";
  }

  public static String setTitle(String title) {
    return "\033]2;" + title + "\007";
  }

  public static String cursor(boolean on) {
    if (on) {
      return "\033[?25h";
    }
    return "\033[?25l";
  }

  public static String moveCursor(int column, int row) {
    return String.format("\033[%d;%dH", row + 1, column + 1);
  }

  public static String clearRemainingLine() {
    return "\033[0;37;40m\033[K";
  }

  public static String color(Color foregroundColor, Color backgroundColor, boolean bold,
    boolean reverse, boolean blink, boolean underline) {

    int foregroundColorValue = getColorValue(foregroundColor, true);
    int backgroundColorValue = getColorValue(backgroundColor, false);

    StringBuilder sb = new StringBuilder();
    if (bold && reverse && blink && !underline) {
      sb.append("\033[0;1;7;5;");
    } else if (bold && reverse && !blink && !underline) {
      sb.append("\033[0;1;7;");
    } else if (!bold && reverse && blink && !underline) {
      sb.append("\033[0;7;5;");
    } else if (bold && !reverse && blink && !underline) {
      sb.append("\033[0;1;5;");
    } else if (bold && !reverse && !blink && !underline) {
      sb.append("\033[0;1;");
    } else if (!bold && reverse && !blink && !underline) {
      sb.append("\033[0;7;");
    } else if (!bold && !reverse && blink && !underline) {
      sb.append("\033[0;5;");
    } else if (bold && reverse && blink) {
      sb.append("\033[0;1;7;5;4;");
    } else if (bold && reverse) {
      sb.append("\033[0;1;7;4;");
    } else if (!bold && reverse && blink) {
      sb.append("\033[0;7;5;4;");
    } else if (bold && blink) {
      sb.append("\033[0;1;5;4;");
    } else if (bold) {
      sb.append("\033[0;1;4;");
    } else if (reverse) {
      sb.append("\033[0;7;4;");
    } else if (blink) {
      sb.append("\033[0;5;4;");
    } else if (underline) {
      sb.append("\033[0;4;");
    } else {
      sb.append("\033[0;");
    }
    sb.append(String.format("%d;%dm", foregroundColorValue, backgroundColorValue));
    return sb.toString();
  }

  private static int getColorValue(Color color, boolean foreground) {
    int baseValue;
    if (foreground) {
      baseValue = 30;
    } else { // background
      baseValue = 40;
    }

    switch (color) {
      case BLACK:
        return baseValue;

      case RED:
        return baseValue + 1;

      case GREEN:
        return baseValue + 2;

      case YELLOW:
        return baseValue + 3;

      case BLUE:
        return baseValue + 4;

      case MAGENTA:
        return baseValue + 5;

      case CYAN:
        return baseValue + 6;

      case WHITE:
        return baseValue + 7;

      default:
        throw new AssertionError();
    }
  }

  public static String normal() {
    return "\033[0;37;40m";
  }
}
