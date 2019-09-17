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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.hbtop.screen.ScreenView;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@Category(SmallTests.class)
@RunWith(MockitoJUnitRunner.class)
public class InputModeScreenPresenterTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(InputModeScreenPresenterTest.class);

  private static final String TEST_INPUT_MESSAGE = "test input message";

  @Mock
  private InputModeScreenView inputModeScreenView;

  @Mock
  private TopScreenView topScreenView;

  @Mock
  private Function<String, ScreenView> resultListener;

  private InputModeScreenPresenter inputModeScreenPresenter;

  @Before
  public void setup() {
    List<String> histories = new ArrayList<>();
    histories.add("history1");
    histories.add("history2");

    inputModeScreenPresenter = new InputModeScreenPresenter(inputModeScreenView,
      TEST_INPUT_MESSAGE, histories, resultListener);
  }

  @Test
  public void testInit() {
    inputModeScreenPresenter.init();

    verify(inputModeScreenView).showInput(eq(TEST_INPUT_MESSAGE), eq(""), eq(0));
  }

  @Test
  public void testCharacter() {
    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');

    InOrder inOrder = inOrder(inputModeScreenView);
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
  }

  @Test
  public void testArrowLeftAndRight() {
    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');
    inputModeScreenPresenter.arrowLeft();
    inputModeScreenPresenter.arrowLeft();
    inputModeScreenPresenter.arrowLeft();
    inputModeScreenPresenter.arrowLeft();
    inputModeScreenPresenter.arrowRight();
    inputModeScreenPresenter.arrowRight();
    inputModeScreenPresenter.arrowRight();
    inputModeScreenPresenter.arrowRight();

    InOrder inOrder = inOrder(inputModeScreenView);
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(0));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
  }

  @Test
  public void testHomeAndEnd() {
    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');
    inputModeScreenPresenter.home();
    inputModeScreenPresenter.home();
    inputModeScreenPresenter.end();
    inputModeScreenPresenter.end();

    InOrder inOrder = inOrder(inputModeScreenView);
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(0));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
  }

  @Test
  public void testBackspace() {
    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');
    inputModeScreenPresenter.backspace();
    inputModeScreenPresenter.backspace();
    inputModeScreenPresenter.backspace();
    inputModeScreenPresenter.backspace();

    InOrder inOrder = inOrder(inputModeScreenView);
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq(""), eq(0));
  }

  @Test
  public void testDelete() {
    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');
    inputModeScreenPresenter.delete();
    inputModeScreenPresenter.arrowLeft();
    inputModeScreenPresenter.delete();

    InOrder inOrder = inOrder(inputModeScreenView);
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
  }

  @Test
  public void testHistories() {
    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');
    inputModeScreenPresenter.arrowUp();
    inputModeScreenPresenter.arrowUp();
    inputModeScreenPresenter.arrowUp();
    inputModeScreenPresenter.arrowDown();
    inputModeScreenPresenter.arrowDown();
    inputModeScreenPresenter.arrowDown();

    InOrder inOrder = inOrder(inputModeScreenView);
    inOrder.verify(inputModeScreenView).showInput(any(), eq("a"), eq(1));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("ab"), eq(2));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("abc"), eq(3));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("history2"), eq(8));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("history1"), eq(8));
    inOrder.verify(inputModeScreenView).showInput(any(), eq("history2"), eq(8));
  }

  @Test
  public void testReturnToTopScreen() {
    when(resultListener.apply(any())).thenReturn(topScreenView);

    inputModeScreenPresenter.character('a');
    inputModeScreenPresenter.character('b');
    inputModeScreenPresenter.character('c');

    assertThat(inputModeScreenPresenter.returnToNextScreen(), is(topScreenView));
    verify(resultListener).apply(eq("abc"));
  }
}
