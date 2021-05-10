package io.github.whippetdb.db.internal;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.internal.DbFile;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

public class DbFileTest {
   @Test
   public void test() throws IOException {
      String path = "tmp/" + getClass().getSimpleName();
      new File(path).delete();
      DbFile dbf = new DbFile(path, Db.Type.Fixed, 6){{
         Util.assertEquals(pageSize, 64);
         Util.assertEquals(size, 1024L);
      }};
      Util.assertEquals(dbf.getVersion(), 0);
      Util.assertEquals(dbf.getRecordedSize(), 1024L);
      Util.assertEquals(dbf.allocatedSize(), 1024L);
      Util.assertEquals(dbf.getDbRoot(), 0L);
      dbf.setDbVersion(123);
      dbf.allocate(1111, false);
      long expectedSize = 1024L + Util.ceil2(1111, 1<<6);
      Util.assertEquals(dbf.allocatedSize(), expectedSize);
      Util.assertEquals(dbf.getRecordedSize(), expectedSize);
      dbf.close();
      
      dbf = new DbFile(path){{
         Util.assertEquals(pageSize, 64);
         Util.assertEquals(size, 1024L);
      }};
      Util.assertEquals(dbf.getDbVersion(), 123);
      Util.assertEquals(dbf.getRecordedSize(), expectedSize);
      Util.assertEquals(dbf.allocatedSize(), 1024L);
      Util.assertEquals(dbf.getDbRoot(), 0L);
      dbf.resume();
      Util.assertEquals(dbf.allocatedSize(), expectedSize);
      dbf.close();
      
      FileUtil.deleteLater(path);
   }
}
