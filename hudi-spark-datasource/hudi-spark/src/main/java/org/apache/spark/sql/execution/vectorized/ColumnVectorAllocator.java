package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.StructType;

public class ColumnVectorAllocator {
  public static WritableColumnVector[] allocateColumns(
      MemoryMode memoryMode, int capacity, StructType schema) {
    if (memoryMode == MemoryMode.OFF_HEAP) {
      return OffHeapColumnVector.allocateColumns(capacity, schema);
    } else {
      return OnHeapColumnVector.allocateColumns(capacity, schema);
    }
  }
}
