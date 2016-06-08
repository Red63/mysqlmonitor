/*
 * Copyright (c) 2010, 2014, Dmall and/or its affiliates. All rights reserved.
 * DMALL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 */
package com.retail.datahub.kafka.sdk.common;

public class KafkaException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public KafkaException() {
    super();
  }

  public KafkaException(String msg) {
    super(msg);
  }

  public KafkaException(String message, Throwable cause) {
    super(message, cause);
  }
}
