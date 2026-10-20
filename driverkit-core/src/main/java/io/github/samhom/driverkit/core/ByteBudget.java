package io.github.samhom.driverkit.core;

import java.io.IOException;
import java.io.OutputStream;

/** Streaming serialization guard without retaining another copy of a request body. */
public final class ByteBudget extends OutputStream {
  private final long maximum;
  private long bytes;

  public ByteBudget(long maximum) {
    if (maximum < 1) throw new IllegalArgumentException("Invalid byte budget");
    this.maximum = maximum;
  }

  @Override
  public void write(int value) throws IOException {
    add(1);
  }

  @Override
  public void write(byte[] value, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset > value.length - length)
      throw new IndexOutOfBoundsException();
    add(length);
  }

  private void add(long length) throws IOException {
    if (length > maximum - bytes) throw new IOException("Serialized request exceeds byte budget");
    bytes += length;
  }

  public long bytes() {
    return bytes;
  }
}
