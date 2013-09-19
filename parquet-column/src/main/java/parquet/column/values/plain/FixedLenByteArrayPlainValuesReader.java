/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.plain;

import static parquet.Log.DEBUG;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.FixedBinary;

public class FixedLenByteArrayPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(FixedLenByteArrayPlainValuesReader.class);
  private byte[] in;
  private int offset;
  private int length;

  public FixedLenByteArrayPlainValuesReader(int length) {
    this.length = length;
  }

  @Override
  public FixedBinary readFixedBytes() {
    try {
      int start = offset;
      offset = start + length;
      return FixedBinary.fromByteArray(in, start, length);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    offset += length;
  }

  @Override
  public int initFromPage(long valueCount, byte[] in, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = in;
    this.offset = offset;
    return in.length;
  }
}
