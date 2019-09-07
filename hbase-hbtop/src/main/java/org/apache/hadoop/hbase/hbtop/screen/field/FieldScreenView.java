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
package org.apache.hadoop.hbase.hbtop.screen.field;

import java.util.EnumMap;
import java.util.List;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.screen.AbstractScreenView;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.hbtop.terminal.KeyPress;
import org.apache.hadoop.hbase.hbtop.terminal.Terminal;
import org.apache.hadoop.hbase.hbtop.terminal.TerminalPrinter;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The screen where we can change the displayed fields, the sort key and the order of the fields.
 */
@InterfaceAudience.Private
public class FieldScreenView extends AbstractScreenView {

  private static final int SCREEN_DESCRIPTION_START_ROW = 0;
  private static final int FIELD_START_ROW = 5;

  private final FieldScreenPresenter fieldScreenPresenter;

  public FieldScreenView(Screen screen, Terminal terminal, Field sortField, List<Field> fields,
    EnumMap<Field, Boolean> fieldDisplayMap, FieldScreenPresenter.ResultListener resultListener,
    ScreenView nextScreenView) {
    super(screen, terminal);
    this.fieldScreenPresenter = new FieldScreenPresenter(this, sortField, fields, fieldDisplayMap,
      resultListener, nextScreenView);
  }

  @Override
  public void init() {
    fieldScreenPresenter.init();
  }

  @Override
  public ScreenView handleKeyPress(KeyPress keyPress) {
    switch (keyPress.getType()) {
      case Escape:
        return fieldScreenPresenter.transitionToNextScreen();

      case ArrowUp:
        fieldScreenPresenter.arrowUp();
        return this;

      case ArrowDown:
        fieldScreenPresenter.arrowDown();
        return this;

      case PageUp:
      case Home:
        fieldScreenPresenter.pageUp();
        return this;

      case PageDown:
      case End:
        fieldScreenPresenter.pageDown();
        return this;

      case ArrowRight:
        fieldScreenPresenter.turnOnMoveMode();
        return this;

      case ArrowLeft:
      case Enter:
        fieldScreenPresenter.turnOffMoveMode();
        return this;

      default:
        // Do nothing
        break;
    }

    if (keyPress.getType() != KeyPress.Type.Character) {
      return this;
    }

    assert keyPress.getCharacter() != null;
    switch (keyPress.getCharacter()) {
      case 'd':
      case ' ':
        fieldScreenPresenter.switchFieldDisplay();
        break;

      case 's':
        fieldScreenPresenter.setSortField();
        break;

      case 'q':
        return fieldScreenPresenter.transitionToNextScreen();

      default:
        // Do nothing
        break;
    }

    return this;
  }

  public void showFieldScreen(String sortFieldHeader, List<Field> fields,
    EnumMap<Field, Boolean> fieldDisplayMap, int currentPosition, int headerMaxLength,
    int descriptionMaxLength, boolean moveMode) {
    showScreenDescription(sortFieldHeader);

    for (int i = 0; i < fields.size(); i ++) {
      Field field = fields.get(i);
      showField(i, field, fieldDisplayMap.get(field), i == currentPosition, headerMaxLength,
        descriptionMaxLength, moveMode);
    }
  }

  public void showScreenDescription(String sortKeyHeader) {
    TerminalPrinter printer = getTerminalPrinter(SCREEN_DESCRIPTION_START_ROW);
    printer.startBold().print("Fields Management").stopBold().endOfLine();
    printer.print("Current Sort Field: ").startBold().print(sortKeyHeader).stopBold().endOfLine();
    printer.print("Navigate with up/down, Right selects for move then <Enter> or Left commits,")
      .endOfLine();
    printer.print("'d' or <Space> toggles display, 's' sets sort. Use 'q' or <Esc> to end!")
      .endOfLine();
  }

  public void showField(int pos, Field field, boolean display, boolean selected,
    int fieldHeaderMaxLength, int fieldDescriptionMaxLength, boolean moveMode) {

    String fieldHeader = String.format("%-" + fieldHeaderMaxLength + "s", field.getHeader());
    String fieldDescription = String.format("%-" + fieldDescriptionMaxLength + "s",
      field.getDescription());

    int row = FIELD_START_ROW + pos;
    TerminalPrinter printer = getTerminalPrinter(row);
    if (selected) {
      String prefix = display ? "* " : "  ";
      if (moveMode) {
        printer.print(prefix);

        if (display) {
          printer.startBold();
        }

        printer.startHighlight()
          .printFormat("%s = %s", fieldHeader, fieldDescription).stopHighlight();

        if (display) {
          printer.stopBold();
        }

        printer.endOfLine();
      } else {
        printer.print(prefix);

        if (display) {
          printer.startBold();
        }

        printer.startHighlight().print(fieldHeader).stopHighlight()
          .printFormat(" = %s", fieldDescription);

        if (display) {
          printer.stopBold();
        }

        printer.endOfLine();
      }
    } else {
      if (display) {
        printer.print("* ").startBold().printFormat("%s = %s", fieldHeader, fieldDescription)
          .stopBold().endOfLine();
      } else {
        printer.printFormat("  %s = %s", fieldHeader, fieldDescription).endOfLine();
      }
    }
  }
}
