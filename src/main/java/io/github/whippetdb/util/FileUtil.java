package io.github.whippetdb.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;

public class FileUtil {
   private static int FILE_ATTRIBUTE_TEMPORARY = 0x100;
   public static boolean isWindows;
   static {
      isWindows = System.getProperty("os.name").startsWith("Windows");
   }
   
   /**
    * Recursively delete file/directory
    */
   public static void delete(String path) throws IOException {
      Path p = Paths.get(path);
      if(!Files.exists(p)) return;
      Files.walkFileTree(p, new SimpleFileVisitor<Path>(){
         public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
         }
         public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
         }
      });
   }
   
   /**
   * Actually deletes a file or renames it to temp name and schedules its deletion.
   * The latter happens if there are open references or dangling mappings to this file,
   * in which case the actual deletion will happen when all references are closed 
   * and all mapping are cleared by the GC.
   */
   public static void remove(String path) throws IOException {
      File f = new File(path);
      if(!f.exists() || f.delete()) return;
      
      checkRegularFile(path);
      
      File tmp = File.createTempFile(f.getName(), "", f.getParentFile());
      tmp.delete();
      f.renameTo(tmp);
      FileChannel fc = FileChannel.open(tmp.toPath(), StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
      fc.close();
   }
   
   /**
    * Actually deletes a file or schedules its deletion.
    * The latter happens if there are open references or dangling mappings to this file,
    * in which case the file becomes inaccessible, and remains such untill all references 
    * are closed and mapping are cleared by the GC.
    * In almost any situation you'd rather use remove().
    */
   public static void deleteLater(String path) throws IOException {
      File f = new File(path);
      if(!f.exists() || f.delete()) return;
      
      checkRegularFile(path);
      
      FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
      fc.close();
   }
   
   /**
    * Windows-only. Advice the OS to avoid writing dirty mapped memory back to disk storage.
    * Uses private JDK features, so it's not guaranteed to work.
    */
   public static void makeTemporary(String path) {
      makeTemporary(new File(path));
   }
   public static void makeTemporary(File f) {
      if(!isWindows) return;
      
      try {
         f.createNewFile();
         DosFileAttributeView view = Files.getFileAttributeView(f.toPath(), DosFileAttributeView.class);
         Class<?> viewClass = view.getClass();
         if(viewClass.getName().endsWith("WindowsFileAttributeViews$Dos")) {
            Method m = viewClass.getMethod("updateAttributes", Integer.TYPE, Boolean.TYPE);
            m.setAccessible(true);
            m.invoke(view, FILE_ATTRIBUTE_TEMPORARY, true);
         }
      }
      catch (Exception e) {}
   }
   
   /**
    * Note: Files.isRegularFile(path) often gives incorrect result, but this always works.
    * No explaination yet.
    */
   private static boolean checkRegularFile(String path) throws IOException {
      return Files.readAttributes(Paths.get(path), BasicFileAttributes.class).isRegularFile();
   }
}
