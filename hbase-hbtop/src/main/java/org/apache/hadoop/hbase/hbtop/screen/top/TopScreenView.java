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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalSize;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The screen that provides a dynamic real-time view for the HBase metrics.
 *
 * This shows the metric {@link Summary} and the metric {@link Record}s. The summary and the
 * metrics are updated periodically (3 seconds by default).
 */
@InterfaceAudience.Private
public class TopScreenView extends AbstractScreenView {

  private static final int SUMMARY_START_ROW = 0;
  private static final int SUMMARY_ROW_NUM = 7;
  private static final int MESSAGE_ROW = 7;
  private static final int RECORD_HEADER_ROW = 8;
  private static final int RECORD_START_ROW = 9;

  private final TopScreenPresenter topScreenPresenter;
  private Integer pageSize;

  public TopScreenView(Screen screen, Terminal terminal, long initialRefreshDelay, Admin admin,
    Mode initialMode, @Nullable List<Field> initialFields, @Nullable Field initialSortField,
    @Nullable Boolean initialAscendingSort, @Nullable List<RecordFilter> initialFilters,
    long numberOfIterations) {
    super(screen, terminal);
    this.topScreenPresenter = new TopScreenPresenter(this, initialRefreshDelay,
      new TopScreenModel(admin, initialMode, initialFields, initialSortField,
        initialAscendingSort, initialFilters), initialFields, numberOfIterations);
  }

  @Override
  public void init() {
    topScreenPresenter.init();
    long delay = topScreenPresenter.refresh(true);
    setTimer(delay);
  }

  @Nullable
  @Override
  public ScreenView handleTimer() {
    long delay = topScreenPresenter.refresh(false);
    setTimer(delay);
    return topScreenPresenter.isIterationFinished() ? null : this;
  }

  @Nullable
  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    switch (keyPress.getType()) {
      case Enter:
        topScreenPresenter.refresh(true);
        return topScreenPresenter.isIterationFinished() ? null : this;

      case ArrowUp:
        topScreenPresenter.arrowUp();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case ArrowDown:
        topScreenPresenter.arrowDown();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case ArrowLeft:
        topScreenPresenter.arrowLeft();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case ArrowRight:
        topScreenPresenter.arrowRight();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case PageUp:
        topScreenPresenter.pageUp();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case PageDown:
        topScreenPresenter.pageDown();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case Home:
        topScreenPresenter.home();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case End:
        topScreenPresenter.end();
        return topScreenPresenter.isIterationFinished() ? null : this;

      case Escape:
        return null;

      default:
        // Do nothing
        break;
    }

    if (keyPress.getType() != KeyPress.Type.Character) {
      return unknownCommandMessage();
    }

    assert keyPress.getCharacter() != null;
    switch (keyPress.getCharacter()) {
      case 'R':
        topScreenPresenter.switchSortOrder();
        break;

      case 'f':
        cancelTimer();
        return topScreenPresenter.transitionToFieldScreen(getScreen(), getTerminal());

      case 'm':
        cancelTimer();
        return topScreenPresenter.transitionToModeScreen(getScreen(), getTerminal());

      case 'h':
        cancelTimer();
        return topScreenPresenter.transitionToHelpScreen(getScreen(), getTerminal());

      case 'd':
        cancelTimer();
        return topScreenPresenter.goToInputModeForRefreshDelay(getScreen(), getTerminal(),
          MESSAGE_ROW);

      case 'o':
        cancelTimer();
        if (keyPress.isCtrl()) {
          return topScreenPresenter.goToFilterDisplayMode(getScreen(), getTerminal(), MESSAGE_ROW);
        }
        return topScreenPresenter.goToInputModeForFilter(getScreen(), getTerminal(), MESSAGE_ROW,
          true);

      case 'O':
        cancelTimer();
        return topScreenPresenter.goToInputModeForFilter(getScreen(), getTerminal(), MESSAGE_ROW,
          false);

      case '=':
        topScreenPresenter.clearFilters();
        break;

      case 'X':
        topScreenPresenter.adjustFieldLength();
        break;

      case 'i':
        topScreenPresenter.drillDown();
        break;

      case 'q':
        return null;

      default:
        return unknownCommandMessage();
    }
    return this;
  }

  @Nullable
  @Override
  public TerminalSize getTerminalSize() {
    TerminalSize terminalSize = super.getTerminalSize();
    if (terminalSize == null) {
      return null;
    }
    updatePageSize(terminalSize);
    return terminalSize;
  }

  @Nullable
  @Override
  public TerminalSize doResizeIfNecessary() {
    TerminalSize terminalSize = super.doResizeIfNecessary();
    if (terminalSize == null) {
      return null;
    }
    updatePageSize(terminalSize);
    return terminalSize;
  }

  private void updatePageSize(TerminalSize terminalSize) {
    pageSize = terminalSize.getRows() - SUMMARY_ROW_NUM - 2;
    if (pageSize < 0) {
      pageSize = 0;
    }
  }

  @Nullable
  public Integer getPageSize() {
    return pageSize;
  }

  public void showTopScreen(Summary summary, List<Header> headers, List<Record> records,
    Record selectedRecord) {
    showSummary(summary);
    clearMessage();
    showHeaders(headers);
    showRecords(headers, records, selectedRecord);
  }

  private void showSummary(Summary summary) {
    TerminalPrinter printer = getTerminalPrinter(SUMMARY_START_ROW);
    printer.print(String.format("HBase hbtop - %s", summary.getCurrentTime())).endOfLine();
    printer.print(String.format("Version: %s", summary.getVersion())).endOfLine();
    printer.print(String.format("Cluster ID: %s", summary.getClusterId())).endOfLine();
    printer.print("RegionServer(s): ")
      .startBold().print(Integer.toString(summary.getServers())).stopBold()
      .print(" total, ")
      .startBold().print(Integer.toString(summary.getLiveServers())).stopBold()
      .print(" live, ")
      .startBold().print(Integer.toString(summary.getDeadServers())).stopBold()
      .print(" dead").endOfLine();
    printer.print("RegionCount: ")
      .startBold().print(Integer.toString(summary.getRegionCount())).stopBold()
      .print(" total, ")
      .startBold().print(Integer.toString(summary.getRitCount())).stopBold()
      .print(" rit").endOfLine();
    printer.print("Average Cluster Load: ")
      .startBold().print(String.format("%.2f", summary.getAverageLoad())).stopBold().endOfLine();
    printer.print("Aggregate Request/s: ")
      .startBold().print(Long.toString(summary.getAggregateRequestPerSecond())).stopBold()
      .endOfLine();
  }

  private void showRecords(List<Header> headers, List<Record> records, Record selectedRecord) {
    TerminalPrinter printer = getTerminalPrinter(RECORD_START_ROW);
    int size;
    if (pageSize != null) {
      size = pageSize;
    } else {
      size = records.size();
    }
    List<String> buf = new ArrayList<>(headers.size());
    for (int i = 0; i < size; i++) {
      if(i < records.size()) {
        Record record = records.get(i);
        buf.clear();
        for (Header header : headers) {
          String value = "";
          if (record.containsKey(header.getField())) {
            value = record.get(header.getField()).asString();
          }

          buf.add(limitLineLength(String.format(header.format(), value), header.getLength()));
        }

        String recordString = String.join(" ", buf);
        if (!recordString.isEmpty()) {
          recordString += " ";
        }

        if (record == selectedRecord) {
          printer.startHighlight().print(recordString).stopHighlight().endOfLine();
        } else {
          printer.print(recordString).endOfLine();
        }
      } else {
        printer.endOfLine();
      }
    }
  }

  private void showHeaders(List<Header> headers) {
    String header = headers.stream()
      .map(h -> String.format(h.format(), h.getField().getHeader()))
      .collect(Collectors.joining(" "));

    if (!header.isEmpty()) {
      header += " ";
    }

    getTerminalPrinter(RECORD_HEADER_ROW).startHighlight().print(header).stopHighlight()
      .endOfLine();
  }

  private String limitLineLength(String line, int length) {
    if (line.length() > length) {
      return line.substring(0, length - 1) + "+";
    }
    return line;
  }

  private void clearMessage() {
    getTerminalPrinter(MESSAGE_ROW).print("").endOfLine();
  }

  private ScreenView unknownCommandMessage() {
    cancelTimer();
    return topScreenPresenter.goToMessageMode(getScreen(), getTerminal(), MESSAGE_ROW,
      "Unknown command - try 'h' for help");
  }
}
