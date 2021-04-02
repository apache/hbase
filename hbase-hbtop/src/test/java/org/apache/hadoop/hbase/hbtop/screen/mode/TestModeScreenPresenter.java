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
package org.apache.hadoop.hbase.hbtop.screen.mode;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.top.TopScreenView;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@Category(SmallTests.class)
@RunWith(MockitoJUnitRunner.class)
public class TestModeScreenPresenter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestModeScreenPresenter.class);

  @Mock
  private ModeScreenView modeScreenView;

  @Mock
  private TopScreenView topScreenView;

  @Mock
  private Consumer<Mode> resultListener;

  private ModeScreenPresenter createModeScreenPresenter(Mode currentMode) {
    return new ModeScreenPresenter(modeScreenView, currentMode, resultListener, topScreenView);
  }

  @Test
  public void testInit() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.init();

    int modeHeaderMaxLength = Mode.REGION_SERVER.getHeader().length();
    int modeDescriptionMaxLength = Mode.REGION_SERVER.getDescription().length();

    verify(modeScreenView).showModeScreen(eq(Mode.REGION), eq(Arrays.asList(Mode.values())),
      eq(Mode.REGION.ordinal()) , eq(modeHeaderMaxLength), eq(modeDescriptionMaxLength));
  }

  @Test
  public void testSelectNamespaceMode() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.arrowUp();
    modeScreenPresenter.arrowUp();

    assertThat(modeScreenPresenter.transitionToNextScreen(true), is(topScreenView));
    verify(resultListener).accept(eq(Mode.NAMESPACE));
  }

  @Test
  public void testSelectTableMode() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.arrowUp();
    assertThat(modeScreenPresenter.transitionToNextScreen(true), is(topScreenView));
    verify(resultListener).accept(eq(Mode.TABLE));
  }

  @Test
  public void testSelectRegionMode() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.NAMESPACE);

    modeScreenPresenter.arrowDown();
    modeScreenPresenter.arrowDown();

    assertThat(modeScreenPresenter.transitionToNextScreen(true), is(topScreenView));
    verify(resultListener).accept(eq(Mode.REGION));
  }

  @Test
  public void testSelectRegionServerMode() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.arrowDown();

    assertThat(modeScreenPresenter.transitionToNextScreen(true), is(topScreenView));
    verify(resultListener).accept(eq(Mode.REGION_SERVER));
  }

  @Test
  public void testCancelSelectingMode() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.arrowDown();
    modeScreenPresenter.arrowDown();

    assertThat(modeScreenPresenter.transitionToNextScreen(false), is(topScreenView));
    verify(resultListener, never()).accept(any());
  }

  @Test
  public void testPageUp() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.pageUp();

    assertThat(modeScreenPresenter.transitionToNextScreen(true), is(topScreenView));
    verify(resultListener).accept(eq(Mode.values()[0]));
  }

  @Test
  public void testPageDown() {
    ModeScreenPresenter modeScreenPresenter = createModeScreenPresenter(Mode.REGION);

    modeScreenPresenter.pageDown();

    assertThat(modeScreenPresenter.transitionToNextScreen(true), is(topScreenView));
    Mode[] modes = Mode.values();
    verify(resultListener).accept(eq(modes[modes.length - 1]));
  }
}
