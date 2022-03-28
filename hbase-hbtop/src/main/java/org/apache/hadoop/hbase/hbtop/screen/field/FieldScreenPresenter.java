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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * The presentation logic for the field screen.
 */
@InterfaceAudience.Private
public class FieldScreenPresenter {

  @FunctionalInterface
  public interface ResultListener {
    void accept(Field sortField, List<Field> fields, EnumMap<Field, Boolean> fieldDisplayMap);
  }

  private final FieldScreenView fieldScreenView;
  private Field sortField;
  private final List<Field> fields;
  private final EnumMap<Field, Boolean> fieldDisplayMap;
  private final ResultListener resultListener;
  private final ScreenView nextScreenView;

  private final int headerMaxLength;
  private final int descriptionMaxLength;

  private int currentPosition;
  private boolean moveMode;

  public FieldScreenPresenter(FieldScreenView fieldScreenView, Field sortField, List<Field> fields,
    EnumMap<Field, Boolean> fieldDisplayMap, ResultListener resultListener,
    ScreenView nextScreenView) {
    this.fieldScreenView = Objects.requireNonNull(fieldScreenView);
    this.sortField = Objects.requireNonNull(sortField);
    this.fields = new ArrayList<>(Objects.requireNonNull(fields));
    this.fieldDisplayMap = new EnumMap<>(Objects.requireNonNull(fieldDisplayMap));
    this.resultListener = Objects.requireNonNull(resultListener);
    this.nextScreenView = Objects.requireNonNull(nextScreenView);

    int headerLength = 0;
    int descriptionLength = 0;
    for (int i = 0; i < fields.size(); i ++) {
      Field field = fields.get(i);

      if (field == sortField) {
        currentPosition = i;
      }

      if (headerLength < field.getHeader().length()) {
        headerLength = field.getHeader().length();
      }

      if (descriptionLength < field.getDescription().length()) {
        descriptionLength = field.getDescription().length();
      }
    }

    headerMaxLength = headerLength;
    descriptionMaxLength = descriptionLength;
  }

  public void init() {
    fieldScreenView.hideCursor();
    fieldScreenView.clearTerminal();
    fieldScreenView.showFieldScreen(sortField.getHeader(), fields, fieldDisplayMap,
      currentPosition, headerMaxLength, descriptionMaxLength, moveMode);
    fieldScreenView.refreshTerminal();
  }

  public void arrowUp() {
    if (currentPosition > 0) {
      currentPosition -= 1;

      if (moveMode) {
        Field tmp = fields.remove(currentPosition);
        fields.add(currentPosition + 1, tmp);
      }

      showField(currentPosition);
      showField(currentPosition + 1);
      fieldScreenView.refreshTerminal();
    }
  }

  public void arrowDown() {
    if (currentPosition < fields.size() - 1) {
      currentPosition += 1;

      if (moveMode) {
        Field tmp = fields.remove(currentPosition - 1);
        fields.add(currentPosition, tmp);
      }

      showField(currentPosition);
      showField(currentPosition - 1);
      fieldScreenView.refreshTerminal();
    }
  }

  public void pageUp() {
    if (currentPosition > 0 && !moveMode) {
      int previousPosition = currentPosition;
      currentPosition = 0;
      showField(previousPosition);
      showField(currentPosition);
      fieldScreenView.refreshTerminal();
    }
  }

  public void pageDown() {
    if (currentPosition < fields.size() - 1  && !moveMode) {
      int previousPosition = currentPosition;
      currentPosition = fields.size() - 1;
      showField(previousPosition);
      showField(currentPosition);
      fieldScreenView.refreshTerminal();
    }
  }

  public void turnOnMoveMode() {
    moveMode = true;
    showField(currentPosition);
    fieldScreenView.refreshTerminal();
  }

  public void turnOffMoveMode() {
    moveMode = false;
    showField(currentPosition);
    fieldScreenView.refreshTerminal();
  }

  public void switchFieldDisplay() {
    if (!moveMode) {
      Field field = fields.get(currentPosition);
      fieldDisplayMap.put(field, !fieldDisplayMap.get(field));
      showField(currentPosition);
      fieldScreenView.refreshTerminal();
    }
  }

  private void showField(int pos) {
    Field field = fields.get(pos);
    fieldScreenView.showField(pos, field, fieldDisplayMap.get(field), pos == currentPosition,
      headerMaxLength, descriptionMaxLength, moveMode);
  }

  public void setSortField() {
    if (!moveMode) {
      Field newSortField = fields.get(currentPosition);
      if (newSortField != this.sortField) {
        this.sortField = newSortField;
        fieldScreenView.showScreenDescription(sortField.getHeader());
        fieldScreenView.refreshTerminal();
      }
    }
  }

  public ScreenView transitionToNextScreen() {
    resultListener.accept(sortField, fields, fieldDisplayMap);
    return nextScreenView;
  }
}
