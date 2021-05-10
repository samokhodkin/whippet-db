package io.github.whippetdb.memory.basic;

import java.nio.ByteBuffer;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.util.Util;

public abstract class SimpleDirectDataIO extends DirectDataIO {
   protected int size;
   
   @Override
   protected void checkRead(long addr, int len) {
      Util.checkRead(addr, len, size);
   }

   protected void ensureSize(long v) {
      if(size >= v) return;
      ensureCapacity(v);
      size = (int)v;
   }

   protected void ensureCapacity(long cap) {
      if(cap > Integer.MAX_VALUE) throw new IllegalArgumentException("Unsupported capacity: " + cap);
      if(data == null) {
         data = ByteBuffer.allocateDirect((int)cap).order(Bytes.BYTE_ORDER);
         return;
      }
      
      int oldCap = data.capacity();
      if(cap <= oldCap) return;
      
      int newCap = (int) Math.min(Integer.MAX_VALUE, Math.max(cap, oldCap*2L + 16));
      ByteBuffer newData = ByteBuffer.allocateDirect(newCap).order(Bytes.BYTE_ORDER);
      Util.copy(data, 0, newData, 0, Math.min(oldCap, size));
      data = newData;
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
}
