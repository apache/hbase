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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The filter display mode in the top screen.
 *
 * Exit if Enter key is pressed.
 */
@InterfaceAudience.Private
public class FilterDisplayModeScreenView extends AbstractScreenView {

  private final int row;
  private final FilterDisplayModeScreenPresenter filterDisplayModeScreenPresenter;

  public FilterDisplayModeScreenView(Screen screen, Terminal terminal, int row,
    List<RecordFilter> filters, ScreenView nextScreenView) {
    super(screen, terminal);
    this.row = row;
    this.filterDisplayModeScreenPresenter =
      new FilterDisplayModeScreenPresenter(this, filters, nextScreenView);
  }

  @Override
  public void init() {
    filterDisplayModeScreenPresenter.init();
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    if (keyPress.getType() == KeyPress.Type.Enter) {
      return filterDisplayModeScreenPresenter.returnToNextScreen();
    }
    return this;
  }

  public void showFilters(List<RecordFilter> filters) {
    String filtersString = "none";
    if (!filters.isEmpty()) {
      filtersString = String.join(" + ",
        filters.stream().map(f -> String.format("'%s'", f)).collect(Collectors.toList()));
    }

    getTerminalPrinter(row).startBold().print("<Enter> to resume, filters: " + filtersString)
      .stopBold().endOfLine();
  }
}
