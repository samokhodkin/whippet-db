package io.github.whippetdb.memory.basic;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.util.Util;

public class SimpleDirectDataBuffer extends SimpleDirectDataIO implements MemDataBuffer {
   public SimpleDirectDataBuffer() {}
   
   public SimpleDirectDataBuffer(int size) {
      ensureCapacity(this.size = size);
   }
   
   // Initialize content with 2-byte chars.
   // For a lightweight wrapper use CharSequenceWrapper
   public SimpleDirectDataBuffer(String s) {
      this(s, 0, s.length());
   }
   
   public SimpleDirectDataBuffer(String s, int off, int len) {
      setSize(size = (len<<1));
      data.asCharBuffer().put(s, off, len);
   }
   
   @Override
   public void setSize(long v) {
      ensureCapacity(v);
      size = (int)v;
   }

   @Override
   public long size() {
      return size;
   }
   
   @Override
   protected void ensureSize(long v) {
      if(v > size) setSize(v);
   }
   
   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkAppend(addr, size);
      ensureSize(addr + len);
   }
}
