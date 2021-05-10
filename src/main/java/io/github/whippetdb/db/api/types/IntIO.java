package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;

public class IntIO implements TypeIO<Integer> {
   @Override
   public Integer size() {
      return 4;
   }
   
   @Override
   public boolean isApplicable(Object o) {
      return o instanceof Integer;
   }

   @Override
   public MemArray convert(Integer obj, MemDataArray tmpBuf) {
      tmpBuf.writeInt(0, obj);
      return tmpBuf;
   }

   @Override
   public void writeObject(Integer obj, MemDataArray buf) {
      buf.writeInt(0, obj);
   }

   @Override
   public Integer readObject(MemDataArray buf) {
      return buf.readInt(0);
   }

   @Override
   public Class<Integer> dataType() {
      return Integer.class;
   }
   
   @Override
   public String toString() {
      return toJson();
   }
}
