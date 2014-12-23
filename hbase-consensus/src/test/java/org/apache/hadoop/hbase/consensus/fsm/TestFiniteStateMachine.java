package org.apache.hadoop.hbase.consensus.fsm;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.classification.InterfaceAudience;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

/**
 * A test class wrapping FiniteStateMachineImpl, exposing some of its internals
 * in order to increase testability.
 */
@InterfaceAudience.Private
class TestableFiniteStateMachine extends FiniteStateMachine {

  public TestableFiniteStateMachine(State defaultStartState) {
    super(TestableFiniteStateMachine.class.getName());
    setStartState(defaultStartState);
  }

  public HashMap<Transition, State> getTransitionMap(
          final State s) {
    return super.getTransitionMap(s);
  }

  public Transition getNextTransition(final State s, final Event e) {
    return super.getNextTransition(s, e);
  }
}

interface ImmutableTestContext {
  public boolean getA();
}

interface MutableTestContext extends ImmutableTestContext, MutableContext {
  public void setA(boolean x);
}

class TestContext implements ImmutableTestContext, MutableTestContext {
  private boolean a;

  public TestContext() {
    this.a = false;
  }

  public boolean getA() { return a; }
  public void setA(boolean x) { a = x; }
}

enum States implements StateType {
  NONE,
  START,
  STOP,
  A,
  B,
  MAX
}

class TestState extends State {
  protected MutableTestContext c;

  public TestState(final States t, final MutableTestContext c) {
    super(t);
    this.c = c;
  }
  public void onEntry(final Event e) {}
  public void onExit(final Event e) {}
}

class Start extends TestState {
  public Start(final MutableTestContext c) {
    super(States.START, c);
  }
}

class Stop extends TestState {
  public Stop(final MutableTestContext c) {
    super(States.STOP, c);
  }
}

class A extends TestState {
  public A(final MutableTestContext c) {
    super(States.A, c);
  }
  public void onEntry(final Event e) {
    if (c != null) c.setA(!c.getA());
  }
}

enum Events implements EventType {
  NONE,
  TYPE1,
  TYPE2,
  MAX
}

enum Transitions implements TransitionType {
  NONE,
  NOOP,
  IS_A,
  ON_TYPE1,
  MAX
}

abstract class TestCondition implements Conditional {
  protected ImmutableTestContext c;

  public TestCondition(final ImmutableTestContext c) {
    this.c = c;
  }

  public abstract boolean isMet(final Event e);
}

class IsNoop extends TestCondition {
  public IsNoop(final ImmutableTestContext c) {
    super(c);
  }
  public boolean isMet(final Event e) { return true; }
}

class IsA extends TestCondition {
  public IsA(final ImmutableTestContext c) {
    super(c);
  }
  public boolean isMet(final Event e) { return c.getA(); }
}

public class TestFiniteStateMachine {
  TestableFiniteStateMachine s;
  TestContext c = new TestContext();

  State START = new Start(c);
  State STOP = new Stop(c);
  State A = new A(c);

  Transition NOOP = new Transition(Transitions.NOOP, new IsNoop(c));
  Transition IS_A = new Transition(Transitions.IS_A, new IsA(c));
  Transition ON_TYPE1 = new Transition(Transitions.ON_TYPE1,
          new OnEvent(Events.TYPE1));

  Event TYPE1 = new Event(Events.TYPE1);

  @Before
  public void createNewStateMachine() {
    s = new TestableFiniteStateMachine(START);
  }

  @Test
  public void shouldAddTransitions() throws Exception {
    s.addTransition(START, STOP, NOOP);
    HashMap<Transition, State> transitionMap = s.getTransitionMap(START);
    assertNotNull(transitionMap);
    assertEquals(STOP, transitionMap.get(NOOP));
  }

  @Test
  public void shouldTransitionOnMetCondition() throws Exception {
    s.addTransition(START, STOP, NOOP);
    assertEquals(NOOP, s.getNextTransition(START, null));
  }

  @Test
  public void shouldIgnoreUnmetConditionals() throws Exception {
    s.addTransition(START, STOP, IS_A);
    assertNull(s.getNextTransition(START, null));
  }

  @Test
  public void shouldTransitionOnEvent() throws Exception {
    s.addTransition(START, STOP, ON_TYPE1);
    assertEquals(ON_TYPE1, s.getNextTransition(START, TYPE1));
  }

  @Test
  public void shouldIgnoreInvalidEvents() throws Exception {
    Event e = new Event(Events.TYPE2);
    s.addTransition(START, STOP, ON_TYPE1);
    assertNull(s.getNextTransition(START, e));
  }

  @Test
  public void shouldBeGreedy() throws Exception {
    s.addTransition(START, A, ON_TYPE1);
    s.addTransition(A, STOP, IS_A);
    s.setStartState(START);
    assertEquals(STOP, s.getNextState(TYPE1));
  }
}
