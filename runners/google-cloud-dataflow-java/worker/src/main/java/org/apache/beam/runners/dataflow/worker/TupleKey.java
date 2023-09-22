package org.apache.beam.runners.dataflow.worker;

import java.util.Objects;

public class TupleKey {

  private Long workToken;
  private String workKey;

  private int hashCode;

  public Long getWorkToken() {
    return workToken;
  }

  public String getWorkKey() {
    return workKey;
  }

  public TupleKey(Long workToken, String key) {
    this.workToken = workToken;
    this.workKey = key;
    this.hashCode = Objects.hash(workToken, key);
  }


  @Override
  public int hashCode() {
    return this.hashCode;
  }
}
