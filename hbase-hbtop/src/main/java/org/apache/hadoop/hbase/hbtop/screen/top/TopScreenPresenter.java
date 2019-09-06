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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.screen.field.FieldScreenView;
import org.apache.hadoop.hbase.hbtop.screen.help.HelpScreenView;
import org.apache.hadoop.hbase.hbtop.screen.mode.ModeScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalSize;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The presentation logic for the top screen.
 */
@InterfaceAudience.Private
public class TopScreenPresenter {
  private final TopScreenView topScreenView;
  private final AtomicLong refreshDelay;
  private long lastRefreshTimestamp;

  private final AtomicBoolean adjustFieldLength = new AtomicBoolean(true);
  private final TopScreenModel topScreenModel;
  private int terminalLength;
  private int horizontalScroll;
  private final Paging paging = new Paging();

  private final EnumMap<Field, Boolean> fieldDisplayMap = new EnumMap<>(Field.class);
  private final EnumMap<Field, Integer> fieldLengthMap = new EnumMap<>(Field.class);

  public TopScreenPresenter(TopScreenView topScreenView, long initialRefreshDelay,
    TopScreenModel topScreenModel) {
    this.topScreenView = Objects.requireNonNull(topScreenView);
    this.refreshDelay = new AtomicLong(initialRefreshDelay);
    this.topScreenModel = Objects.requireNonNull(topScreenModel);

    initFieldDisplayMapAndFieldLengthMap();
  }

  public void init() {
    terminalLength = topScreenView.getTerminalSize().getColumns();
    paging.updatePageSize(topScreenView.getPageSize());
    topScreenView.hideCursor();
  }

  public long refresh(boolean force) {
    if (!force) {
      long delay = System.currentTimeMillis() - lastRefreshTimestamp;
      if (delay < refreshDelay.get()) {
        return refreshDelay.get() - delay;
      }
    }

    TerminalSize newTerminalSize = topScreenView.doResizeIfNecessary();
    if (newTerminalSize != null) {
      terminalLength = newTerminalSize.getColumns();
      paging.updatePageSize(topScreenView.getPageSize());
      topScreenView.clearTerminal();
    }

    topScreenModel.refreshMetricsData();
    paging.updateRecordsSize(topScreenModel.getRecords().size());

    adjustFieldLengthIfNeeded();

    topScreenView.showTopScreen(topScreenModel.getSummary(), getDisplayedHeaders(),
      getDisplayedRecords(), getSelectedRecord());

    topScreenView.refreshTerminal();

    lastRefreshTimestamp = System.currentTimeMillis();
    return refreshDelay.get();
  }

  public void adjustFieldLength() {
    adjustFieldLength.set(true);
    refresh(true);
  }

  private void adjustFieldLengthIfNeeded() {
    if (adjustFieldLength.get()) {
      adjustFieldLength.set(false);

      for (Field f : topScreenModel.getFields()) {
        if (f.isAutoAdjust()) {
          int maxLength = topScreenModel.getRecords().stream()
            .map(r -> r.get(f).asString().length())
            .max(Integer::compareTo).orElse(0);
          fieldLengthMap.put(f, Math.max(maxLength, f.getHeader().length()));
        }
      }
    }
  }

  private List<Header> getDisplayedHeaders() {
    List<Field> displayFields =
      topScreenModel.getFields().stream()
        .filter(fieldDisplayMap::get).collect(Collectors.toList());

    if (displayFields.isEmpty()) {
      horizontalScroll = 0;
    } else if (horizontalScroll > displayFields.size() - 1) {
      horizontalScroll = displayFields.size() - 1;
    }

    List<Header> ret = new ArrayList<>();

    int length = 0;
    for (int i = horizontalScroll; i < displayFields.size(); i++) {
      Field field = displayFields.get(i);
      int fieldLength = fieldLengthMap.get(field);

      length += fieldLength + 1;
      if (length > terminalLength) {
        break;
      }
      ret.add(new Header(field, fieldLength));
    }

    return ret;
  }

  private List<Record> getDisplayedRecords() {
    List<Record> ret = new ArrayList<>();
    for (int i = paging.getPageStartPosition(); i < paging.getPageEndPosition(); i++) {
      ret.add(topScreenModel.getRecords().get(i));
    }
    return ret;
  }

  private Record getSelectedRecord() {
    if (topScreenModel.getRecords().isEmpty()) {
      return null;
    }
    return topScreenModel.getRecords().get(paging.getCurrentPosition());
  }

  public void arrowUp() {
    paging.arrowUp();
    refresh(true);
  }

  public void arrowDown() {
    paging.arrowDown();
    refresh(true);
  }

  public void pageUp() {
    paging.pageUp();
    refresh(true);
  }

  public void pageDown() {
    paging.pageDown();
    refresh(true);
  }

  public void arrowLeft() {
    if (horizontalScroll > 0) {
      horizontalScroll -= 1;
    }
    refresh(true);
  }

  public void arrowRight() {
    if (horizontalScroll < getHeaderSize() - 1) {
      horizontalScroll += 1;
    }
    refresh(true);
  }

  public void home() {
    if (horizontalScroll > 0) {
      horizontalScroll = 0;
    }
    refresh(true);
  }

  public void end() {
    int headerSize = getHeaderSize();
    horizontalScroll = headerSize == 0 ? 0 : headerSize - 1;
    refresh(true);
  }

  private int getHeaderSize() {
    return (int) topScreenModel.getFields().stream()
      .filter(fieldDisplayMap::get).count();
  }

  public void switchSortOrder() {
    topScreenModel.switchSortOrder();
    refresh(true);
  }

  public ScreenView transitionToHelpScreen(Screen screen, Terminal terminal) {
    return new HelpScreenView(screen, terminal, refreshDelay.get(), topScreenView);
  }

  public ScreenView transitionToModeScreen(Screen screen, Terminal terminal) {
    return new ModeScreenView(screen, terminal, topScreenModel.getCurrentMode(), this::switchMode,
      topScreenView);
  }

  public ScreenView transitionToFieldScreen(Screen screen, Terminal terminal) {
    return new FieldScreenView(screen, terminal,
      topScreenModel.getCurrentSortField(), topScreenModel.getFields(),
      fieldDisplayMap,
      (sortKey, fields, fieldDisplayMap) -> {
        topScreenModel.setSortFieldAndFields(sortKey, fields);
        this.fieldDisplayMap.clear();
        this.fieldDisplayMap.putAll(fieldDisplayMap);
      }, topScreenView);
  }

  private void switchMode(Mode nextMode) {
    topScreenModel.switchMode(nextMode, null, false);
    reset();
  }

  public void drillDown() {
    Record selectedRecord = getSelectedRecord();
    if (selectedRecord == null) {
      return;
    }
    if (topScreenModel.drillDown(selectedRecord)) {
      reset();
      refresh(true);
    }
  }

  private void reset() {
    initFieldDisplayMapAndFieldLengthMap();
    adjustFieldLength.set(true);
    paging.init();
    horizontalScroll = 0;
    topScreenView.clearTerminal();
  }

  private void initFieldDisplayMapAndFieldLengthMap() {
    fieldDisplayMap.clear();
    fieldLengthMap.clear();
    for (FieldInfo fieldInfo : topScreenModel.getFieldInfos()) {
      fieldDisplayMap.put(fieldInfo.getField(), fieldInfo.isDisplayByDefault());
      fieldLengthMap.put(fieldInfo.getField(), fieldInfo.getDefaultLength());
    }
  }

  public ScreenView goToMessageMode(Screen screen, Terminal terminal, int row, String message) {
    return new MessageModeScreenView(screen, terminal, row, message, topScreenView);
  }

  public ScreenView goToInputModeForRefreshDelay(Screen screen, Terminal terminal, int row) {
    return new InputModeScreenView(screen, terminal, row,
      "Change refresh delay from " + (double) refreshDelay.get() / 1000 + " to", null,
      (inputString) -> {
        if (inputString.isEmpty()) {
          return topScreenView;
        }

        double delay;
        try {
          delay = Double.valueOf(inputString);
        } catch (NumberFormatException e) {
          return goToMessageMode(screen, terminal, row, "Unacceptable floating point");
        }

        refreshDelay.set((long) (delay * 1000));
        return topScreenView;
      });
  }

  public ScreenView goToInputModeForFilter(Screen screen, Terminal terminal, int row,
    boolean ignoreCase) {
    return new InputModeScreenView(screen, terminal, row,
      "add filter #" + (topScreenModel.getFilters().size() + 1) +
        " (" + (ignoreCase ? "ignoring case" : "case sensitive") + ") as: [!]FLD?VAL",
      topScreenModel.getFilterHistories(),
      (inputString) -> {
        if (inputString.isEmpty()) {
          return topScreenView;
        }

        if (!topScreenModel.addFilter(inputString, ignoreCase)) {
          return goToMessageMode(screen, terminal, row, "Unacceptable filter expression");
        }

        paging.init();
        return topScreenView;
      });
  }

  public void clearFilters() {
    topScreenModel.clearFilters();
    paging.init();
    refresh(true);
  }

  public ScreenView goToFilterDisplayMode(Screen screen, Terminal terminal, int row) {
    return new FilterDisplayModeScreenView(screen, terminal, row, topScreenModel.getFilters(),
      topScreenView);
  }
}
