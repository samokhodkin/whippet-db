package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;

public class LongIO implements TypeIO<Long> {
   @Override
   public Integer size() {
      return 8;
   }

   @Override
   public MemArray convert(Long obj, MemDataArray tmpBuf) {
      tmpBuf.writeLong(0, obj);
      return tmpBuf;
   }

   @Override
   public void writeObject(Long obj, MemDataArray buf) {
      buf.writeLong(0, obj);
   }

   @Override
   public Long readObject(MemDataArray buf) {
      return buf == null? null : buf.readLong(0);
   }
   
   @Override
   public boolean isApplicable(Object o) {
      return o instanceof Long;
   }

   @Override
   public Class<Long> dataType() {
      return Long.class;
   }
   
   @Override
   public String toString() {
      return toJson();
   }
}
