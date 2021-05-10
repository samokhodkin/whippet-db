package io.github.whippetdb.memory.basic;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.util.Util;

public class SimpleDirectDataSpace extends SimpleDirectDataIO implements MemDataSpace {
   public SimpleDirectDataSpace(){}
   
   public SimpleDirectDataSpace(int reserved){
      ensureCapacity(size = reserved);
   }
   
   @Override
   public long allocate(int len, boolean clear) {
      long addr = size;
      long newSize = addr + len;
      ensureCapacity(newSize);
      size = (int)newSize;
      // new space is already clear
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
