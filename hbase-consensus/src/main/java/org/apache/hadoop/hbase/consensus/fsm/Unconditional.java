package org.apache.hadoop.hbase.consensus.fsm;

public class Unconditional implements Conditional {

  @Override
  public boolean isMet(Event e){
    return true;
  }
}
