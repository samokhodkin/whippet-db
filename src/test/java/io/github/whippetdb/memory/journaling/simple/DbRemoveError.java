package io.github.whippetdb.memory.journaling.simple;

import java.io.IOException;
import java.util.Map;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.Db.Type;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.db.internal.DbFile;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.journaling.simple.SimpleJournalingMemSpace;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

@SuppressWarnings("unused")
public class DbRemoveError {
   static final SimpleDirectDataBuffer key = new SimpleDirectDataBuffer(8);
   static final String path = "tmp/" + DbRemoveError.class.getSimpleName();
   
   public static void main(String[] args) throws Exception {
      runBuilder();
   }
   
   static void runBuilder() throws IOException{
      System.out.println("runBuilder()");
      
      FileUtil.remove(path);
      FileUtil.remove(path + ".wal");
      
      DbBuilder<Long, Long> builder = new DbBuilder<>(new LongIO(), new LongIO());
      builder.journaling(true);
      builder.create(path);
      Db db = builder.db();
      
      SimpleJournalingMemSpace ms = (SimpleJournalingMemSpace)Util.getField(db, "val$ms");
      DbFile dbf = (DbFile)Util.getField(ms, "dbf");
      MemMap.Cursor cursor = (MemMap.Cursor)Util.getField(db, "val$db");
      
      System.out.println("ms after creation: " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
      
      int N = 4; //100 //100_000;
      Map<Long, Long> map = builder.asMap();
      
      for(long v = N; v --> 0;) {
         System.out.println("insert " + v + ":" + (map.put(v, v)==null));
      }
      Util.assertEquals(map.size(), N);
      
      System.out.println("ms after inserts: " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
      
      System.out.println("Scan:");
      map.forEach((k,v) -> {
         System.out.println(k + " -> " + v);
      });
      System.out.println("ms after scan: " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
      
      System.out.println("remove 3: " + map.remove(3L));
      System.out.println("ms -> " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
      
      System.out.println("remove 2: " + map.remove(2L));
      System.out.println("ms -> " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
      
//      System.out.println("Scan:");
//      cursor.scan(() -> {
//         System.out.println(cursor.key.readLong(0) + " -> " + cursor.value.readLong(0));
//         return false;
//      });
//      System.out.println("ms after scan: " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
//      
//      System.out.println("remove " + 3 + ": " + cursor.seek(key(3), 0)); cursor.delete(); ms.commit();
//      System.out.println("ms -> " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
//      
//      System.out.println("remove " + 2 + ": " + cursor.seek(key(2), 0)); cursor.delete();
//      System.out.println("ms -> " + ms.allocatedSize() + "/" + FastHash.hash64(ms, 0, (int)ms.allocatedSize()));
      
   }
   
   static MemDataArray key(long k) {
      key.writeLong(0, k);
      return key;
   }
}
