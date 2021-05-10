package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;

public class BooleanIO implements TypeIO<Boolean> {
   @Override
   public Integer size() {
      return 1;
   }
   
   @Override
   public Class<Boolean> dataType() {
      return Boolean.class;
   }
   
   @Override
   public boolean isApplicable(Object o) {
      return o instanceof Boolean;
   }

   @Override
   public MemArray convert(Boolean obj, MemDataArray tmpBuf) {
      tmpBuf.writeByte(0, obj? 1: 0);
      return tmpBuf;
   }

   @Override
   public void writeObject(Boolean obj, MemDataArray buf) {
      buf.writeByte(0, obj? 1: 0);
   }

   @Override
   public Boolean readObject(MemDataArray buf) {
      return buf.readByte(0) == 1? true: false;
   }
}
