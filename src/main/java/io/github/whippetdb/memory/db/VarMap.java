package io.github.whippetdb.memory.db;

import static io.github.whippetdb.util.FastHash.*;

import java.util.function.Function;

import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.*;
import io.github.whippetdb.util.LongList;

public class VarMap {
   // root record dbRootAddr(8) + dataLogPageSize(1) + hash64(dbRootAddr + dataPageSize) + dataCache(8)
   private static int DB_BASE_ADDR_OFF = 0;
   private static int KEY_PAGE_SIZE_OFF = DB_BASE_ADDR_OFF + 8;
   private static int VALUE_PAGE_SIZE_OFF = KEY_PAGE_SIZE_OFF + 2;
   private static int ROOT_HASH_OFF = VALUE_PAGE_SIZE_OFF + 2;
   private static int KEY_DATA_CACHE_OFF = ROOT_HASH_OFF + 8;
   private static int VALUE_DATA_CACHE_OFF = KEY_DATA_CACHE_OFF + 8;
   private static int ROOT_RECORD_SIZE = VALUE_DATA_CACHE_OFF + 8;
   
   // MemMap key; consists of only 8-byte hash
   private static int MM_KEY_SIZE = 8;
   
   // MemMap value; keySize(8) + keyRoot(8) + valueSize(8) + valueRoot(8)
   private static int KEY_SIZE_OFF = 0;
   private static int KEY_ROOT_OFF = KEY_SIZE_OFF + 8;
   private static int VALUE_SIZE_OFF = KEY_ROOT_OFF + 8;
   private static int VALUE_ROOT_OFF = VALUE_SIZE_OFF + 8;
   private static int MM_VALUE_SIZE = VALUE_ROOT_OFF + 8;
   
   private static int MM_VALUE_OFF = MemMap.KEY_OFF + MM_KEY_SIZE;
   
   public final long baseAddr;
   private final MemMap db;
   private final int keyPageSize, valuePageSize;
   private final long keyDataCacheAddr, valueDataCacheAddr;
   
   public VarMap(MemDataSpace ms, int keyPageSize, int valuePageSize, int maxListSize) {
      this.keyPageSize = keyPageSize;
      this.valuePageSize = valuePageSize;
      baseAddr = ms.allocate(ROOT_RECORD_SIZE, true);
      db = new MemMap(ms, MM_KEY_SIZE, MM_VALUE_SIZE, maxListSize);
      ms.writeLong(baseAddr + DB_BASE_ADDR_OFF, db.baseAddr);
      ms.writeShort(baseAddr + KEY_PAGE_SIZE_OFF, keyPageSize);
      ms.writeShort(baseAddr + VALUE_PAGE_SIZE_OFF, valuePageSize);
      ms.writeLong(baseAddr + ROOT_HASH_OFF, hash64(ms, baseAddr, ROOT_HASH_OFF));
      keyDataCacheAddr = baseAddr + KEY_DATA_CACHE_OFF;
      valueDataCacheAddr = baseAddr + VALUE_DATA_CACHE_OFF;
   }
   
   public VarMap(MemDataSpace ms, long baseAddr) {
      this.baseAddr = baseAddr;
      long dbBaseAddr = ms.readLong(baseAddr + DB_BASE_ADDR_OFF);
      keyPageSize = ms.readByte(baseAddr + KEY_PAGE_SIZE_OFF);
      valuePageSize = ms.readByte(baseAddr + VALUE_PAGE_SIZE_OFF);
      if(ms.readLong(baseAddr + ROOT_HASH_OFF) != hash64(ms, baseAddr, ROOT_HASH_OFF)) {
         throw new IllegalArgumentException("Not a root record: " + baseAddr);
      }
      keyDataCacheAddr = baseAddr + KEY_DATA_CACHE_OFF;
      valueDataCacheAddr = baseAddr + VALUE_DATA_CACHE_OFF;
      db = new MemMap(ms, dbBaseAddr);
   }
   
   public class Cursor {
      public final VarData value;
      
      private final MemMap.Cursor parent;
      private final MemDataIO hash = new SimpleDirectDataBuffer(8);
      private final MemDataSpace ms;
      
      public Cursor(MemDataSpace ms) {
         this.ms = ms;
         parent = db.new Cursor(ms);
         value = new VarData(ms, valueDataCacheAddr, valuePageSize);
      }
      
      public boolean seek(MemArray key) {
         return seek(key, 0, (int)key.size());
      }
      
      public boolean seek(MemIO key, long off, int len) {
         hash.writeLong(0, fnb64(key, off, len));
         if(!parent.seek(hash, 0)) return false;
         do{
            if(checkKey(key, off, len)){
               connectValue(value);
               return true;
            }
         }
         while(parent.nextValue());
         return false;
      }
      
      public boolean put(MemArray key) {
         return put(key, 0, (int)key.size());
      }
      
      public boolean put(MemIO key, long off, int len) {
         hash.writeLong(0, fnb64(key, off, len));
         if(!parent.put(hash, 0)) { // hash exists
            do {
               if(checkKey(key, off, len)){
                  connectValue(value);
                  return false;
               }
            }
            while(parent.nextValue());
            parent.addValue(); // value not found, create new one
         }
         parent.value.fillLong(0, MM_VALUE_SIZE, 0);
         putKey(key, off, len);
         connectValue(value);
         return true;
      }
      
      public void delete() {
         deleteKey();
         deleteValue();
         parent.delete();
      }
      
      public void scan(Function<MemDataArray,Boolean> feed) {
         VarData key = new VarData(ms, keyDataCacheAddr, keyPageSize){
            @Override
            public void setSize(long v) {
               throw new UnsupportedOperationException();
            }
         };
         parent.scan(()->{
            connectKey(key);
            connectValue(value);
            return feed.apply(key);
         });
      }
      
      public void print() {
         scan(k->{
            System.out.println(k + " -> " + value);
            return false;
         });
      }
      
      public LongList mmStat() {
         return parent.stat();
      }
      
      private void connectKey(VarData key) {
         key.connect(parent.currentAddr + MM_VALUE_OFF + KEY_SIZE_OFF, parent.currentAddr + MM_VALUE_OFF + KEY_ROOT_OFF);
      }
      
      private void connectValue(VarData value) {
         value.connect(parent.currentAddr + MM_VALUE_OFF + VALUE_SIZE_OFF, parent.currentAddr + MM_VALUE_OFF + VALUE_ROOT_OFF);
      }
      
      private boolean checkKey(MemIO key, long off, int len) {
         long keySize = ms.readLong(parent.currentAddr + MM_VALUE_OFF + KEY_SIZE_OFF);
         if(len != keySize) return false;
         long keyRoot = parent.currentAddr + MM_VALUE_OFF + KEY_ROOT_OFF;
         return VarData.equals(ms, keyRoot, keyPageSize, key, off, len);
      }
      
      private void putKey(MemIO key, long off, int len) {
         long keyRoot = parent.currentAddr + MM_VALUE_OFF + KEY_ROOT_OFF;
         VarData.create(ms, keyRoot, keyDataCacheAddr, keyPageSize, key, off, len);
         ms.writeLong(parent.currentAddr + MM_VALUE_OFF + KEY_SIZE_OFF, len);
      }
      
      private void deleteKey() {
         long keyRoot = parent.currentAddr + MM_VALUE_OFF + KEY_ROOT_OFF;
         VarData.delete(ms, keyRoot, keyDataCacheAddr);
         ms.writeLong(parent.currentAddr + MM_VALUE_OFF + KEY_SIZE_OFF, 0);
      }
      
      private void deleteValue() {
         long valueRoot = parent.currentAddr + MM_VALUE_OFF + VALUE_ROOT_OFF;
         VarData.delete(ms, valueRoot, valueDataCacheAddr);
         ms.writeLong(parent.currentAddr + MM_VALUE_OFF + VALUE_SIZE_OFF, 0);
      }
   }
}
