package io.github.whippetdb.db.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.ProxyDataArray;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.db.MemMap.Cursor;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.Util;

/**
 * Represents fixed-size fields as limited var-size
 */
@SuppressWarnings("unused")
public class HybridDb implements Db {
   protected final Db parent;
   protected final boolean hybridKey, hybridValue;
   protected final int keySize, valueSize;
   
   private final HybridKeyWrapper keyBuf = new HybridKeyWrapper();
   private final HybridValue valueBuf = new HybridValue();
   
   // Both key and value of the parent must be fixed-size
   public HybridDb(Db parent, boolean hybridKey, boolean hybridValue) {
      this.parent = parent;
      this.hybridKey = hybridKey;
      this.hybridValue = hybridValue;
      keySize = parent.keySize();
      valueSize = parent.valueSize();
   }

   public Integer keySize() {
      return hybridKey? null: keySize;
   }

   public Integer valueSize() {
      return hybridValue? null: valueSize;
   }
   
   @Override
   public Integer maxKeySize() {
      return hybridKey? keySize - 1: keySize;
   }
   
   @Override
   public Integer maxValueSize() {
      return hybridValue? valueSize - 1: valueSize;
   }

   public <T> T seek(MemIO key, long keyOff, int keyLen, SeekHandler<T> handler) {
      return hybridKey?
            parent.seek(hKey(key, keyOff, keyLen), modify(handler)) :
            parent.seek(key, keyOff, keyLen, modify(handler));
   }

   public <T> T put(MemIO key, long keyOff, int keyLen, PutHandler<T> handler) {
      return hybridKey?
            parent.put(hKey(key, keyOff, keyLen), modify(handler)) :
            parent.put(key, keyOff, keyLen, modify(handler));
   }

   public void scan(ScanHandler handler) {
      HybridValue hkey = new HybridKey();
      HybridValue hval = new HybridValue();
      parent.scan((key, value, delete) -> {
         hkey.setParent(key, keySize);
         hval.setParent(value, valueSize);
         return handler.run(hkey, hval, delete);
      });
   }

   public void commit() {
      parent.commit();
   }
   
   @Override
   public void discard() {
      parent.discard();
   }
   
   public void flush() {
      parent.flush();
   }
   
   public void close() {
      parent.close();
   }
   
   public long allocatedSize() {
      return parent.allocatedSize();
   }

   private <T> SeekHandler<T> modify(SeekHandler<T> handler) {
      return hybridValue? (data, delete) -> handler.run(hValue(data, false), delete) : handler;
   }
   
   private <T> PutHandler<T> modify(PutHandler<T> handler) {
      return hybridValue? (created, data, delete) -> handler.run(created, hValue(data, created), delete) : handler;
   }
   
   private MemArray hKey(MemIO key, long keyOff, int keyLen) {
      keyBuf.setData(keySize, key, keyOff, keyLen);
      return keyBuf;
   }
   
   private MemDataArray hValue(MemDataArray data, boolean reset) {
      if(data == null) return null;
      valueBuf.setParent(data, valueSize);
      if(reset) valueBuf.setSize(0);
      return valueBuf;
   }
   
   static class HybridValue extends ProxyDataArray implements MemDataBuffer {
      private long parentSize;
      private long cachedSize;
      
      void setParent(MemDataIO parent, long size) {
         this.parent = parent;
         parentSize = size;
         cachedSize = parent.readByte(0);
      }
      public void setSize(long v) {
         if(v >= parentSize) throw new IllegalArgumentException("Requested size " + v + " exceeds max allowed value " + (parentSize - 1));
         parent.writeByte(0, (int) (cachedSize = v));
      }
      public long size() {
         return cachedSize;
      }
      protected long offset() {
         return 1;
      }
      @Override
      protected void checkWrite(long addr, int len) {
         Util.checkAppend(addr, cachedSize);
         if(addr + len > cachedSize) setSize(addr + len);
      }
   }
   
   static class HybridKey extends HybridValue {
      @Override
      protected void checkWrite(long addr, int len) {
         throw new UnsupportedOperationException();
      }
   }
}
