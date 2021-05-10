package io.github.whippetdb.memory.basic;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.util.Util;

public class SimpleHeapDataSpace extends SimpleHeapDataIO implements MemDataSpace{
   @Override
   public long allocate(int size, boolean clear) {
      long addr = this.size;
      ensureSize(addr + size);
      if(clear) fillLong(addr, size, 0);
      return addr;
   }

   @Override
   public long allocatedSize() {
      return size;
   }

   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkWrite(addr, len, size);
   }
}
