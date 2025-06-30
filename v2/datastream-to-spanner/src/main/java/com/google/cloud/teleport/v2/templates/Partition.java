package com.google.cloud.teleport.v2.templates;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** A simple POJO to hold the lower (inclusive) and upper (exclusive) bounds of a partition. */
@DefaultCoder(AvroCoder.class)
public class Partition implements Serializable {
  public final long lowerBound;
  public final long upperBound;

  // No-arg constructor for coder
  public Partition() {
    this.lowerBound = 0;
    this.upperBound = 0;
  }

  public Partition(long lowerBound, long upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  @Override
  public String toString() {
    return "Partition{" + "lowerBound=" + lowerBound + ", upperBound=" + upperBound + '}';
  }
}

