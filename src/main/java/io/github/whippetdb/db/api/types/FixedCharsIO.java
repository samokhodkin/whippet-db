package io.github.whippetdb.db.api.types;

import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.basic.StringWrapper;

/**
 * Fixed-size strings.
 */

public class FixedCharsIO implements TypeIO<CharSequence> {
   private final int strLen;
   
   public FixedCharsIO(int strLen) {
      this.strLen = strLen;
   }
   
   @Override
   public Integer size() {
      return strLen << 1;
   }

   @Override
   public MemArray convert(CharSequence obj, MemDataArray tmpBuf) {
      return new StringWrapper(obj);
   }

   @Override
   public void writeObject(CharSequence obj, MemDataArray buf) {
      buf.write(0, obj);
   }

   @Override
   public CharSequence readObject(MemDataArray buf) {
      return buf == null? null: buf.readString(0, strLen);
   }

   @Override
   public boolean isApplicable(Object o) {
      return o instanceof CharSequence;
   }

   @Override
   public Class<CharSequence> dataType() {
      return CharSequence.class;
   }
   
   @Override
   public String toString() {
      return toJson();
   }
}
