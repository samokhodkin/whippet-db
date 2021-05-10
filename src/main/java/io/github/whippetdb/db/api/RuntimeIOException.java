package io.github.whippetdb.db.api;

import java.io.IOException;

public class RuntimeIOException extends RuntimeException {
   private static final long serialVersionUID = 1L;

   public RuntimeIOException(IOException cause) {
      super(cause.getMessage(), cause);
   }
   
   @Override
   public synchronized IOException getCause() {
      return (IOException) super.getCause();
   }
}
