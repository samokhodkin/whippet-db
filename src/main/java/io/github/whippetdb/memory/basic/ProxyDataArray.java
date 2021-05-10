package io.github.whippetdb.memory.basic;

import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.util.Util;

public abstract class ProxyDataArray extends ProxyDataIO implements MemDataArray {
   public abstract long size();
   
   protected void checkRead(long addr, int len) {
      Util.checkRead(addr, len, size());
   }
   
   protected void checkWrite(long addr, int len) {
      Util.checkWrite(addr, len, size());
   }
   
   @Override
   public String toString() {
      return toString(0, size());
   }
}
