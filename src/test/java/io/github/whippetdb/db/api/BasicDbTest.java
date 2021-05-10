package io.github.whippetdb.db.api;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.TypeIO;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.db.internal.DbFile;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

import static org.junit.Assert.*;

public class BasicDbTest {
   static final TypeIO<CharSequence> keyType = new CharsIO(8);
   static final TypeIO<CharSequence> valueType = new CharsIO(15);
   
   final String dbPath = "tmp/" + getClass().getSimpleName();
   static {
      new File("tmp").mkdir();
   }
   
   @Test
   public void test() throws IOException {
      DbBuilder<CharSequence, CharSequence> builder = new DbBuilder<>(keyType, valueType);
      Map<CharSequence,CharSequence> map;
      int N = 150_000;
      
      System.out.println("Creating");
      map = builder.create(dbPath).asMap();
      
      Db parentDb = (Db)Util.getField(builder.db(), "parent");
      DbFile dbf = (DbFile) Util.getField(parentDb, "val$ms");
      assertNotNull(parentDb.keySize());
      assertNotNull(parentDb.valueSize());
      assertEquals(parentDb.keySize().intValue(), builder.keyType().maxSize() + 1);
      assertEquals(parentDb.valueSize().intValue(), builder.valueType().maxSize() + 1);
      assertEquals(dbf.readInt(dbf.getDbRoot() + MemMap.KEY_SIZE_OFF), parentDb.keySize().intValue());
      assertEquals(dbf.readInt(dbf.getDbRoot() + MemMap.VALUE_SIZE_OFF), parentDb.valueSize().intValue());
      
      // insert N records
      for(int i = N; i --> 0;) {
         assertNull(map.put("" + i, "Value("+i+")"));
      }
      Util.assertEquals(map.size(), N);
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      
      builder.db().close();
      
      // Reopen
      map = builder.open(dbPath).asMap();
      
      parentDb = (Db)Util.getField(builder.db(), "parent");
      dbf = (DbFile) Util.getField(parentDb, "val$ms");
      assertNotNull(parentDb.keySize());
      assertNotNull(parentDb.valueSize());
      assertEquals(parentDb.keySize().intValue(), builder.keyType().maxSize() + 1);
      assertEquals(parentDb.valueSize().intValue(), builder.valueType().maxSize() + 1);
      assertEquals(dbf.readInt(dbf.getDbRoot() + MemMap.KEY_SIZE_OFF), parentDb.keySize().intValue());
      assertEquals(dbf.readInt(dbf.getDbRoot() + MemMap.VALUE_SIZE_OFF), parentDb.valueSize().intValue());
      
      for(int i = N; i --> 0;) {
         assertEquals(String.valueOf(map.get("" + i)), "Value("+i+")");
      }
      Util.assertEquals(map.size(), N);
      assertTrue("avg record size should fit in 150 bytes", builder.db().allocatedSize()/N < 150);
      
      int M = N/2;
      for(int i = M; i --> 0;) {
         assertEquals(String.valueOf(map.remove("" + i)), "Value("+i+")");
      }
      
      assertEquals(map.size(), N-M);
      
      builder.db().close();
      FileUtil.deleteLater(dbPath);
   }
}
