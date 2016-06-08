package com.retail.datahub.kafka.sdk.api;


public final class Preconditions {
  private Preconditions() {
  }


  public static <T> T checkNotNull(T obj, String name) {
    if (obj == null) {
      String msg = String.format("parameter '%s' cannot be null", name);
      throw new NullPointerException(msg);
    }
    return obj;
  }


  public static void checkArgument(
          boolean expression, String errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(errorMessage);
    }
  }
}