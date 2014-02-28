package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;

public class WTFInputStream extends FSInputStream {
	
	public WTFInputStream(long fd) {
		// TODO Auto-generated constructor stub
	}

	  @Override
	  public synchronized long getPos() throws IOException {
	    return 0;
	  }

	  @Override
	  public synchronized int available() throws IOException {
	    return 0;
	  }

	  @Override
	  public synchronized void seek(long targetPos) throws IOException {

	  }

	  @Override
	  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
	    return false;
	  }

	  @Override
	  public synchronized int read() throws IOException {
		  return 0;
	  }

	  @Override
	  public synchronized int read(byte buf[], int off, int len) throws IOException {
	    return 0;
	  }

	  /**
	   * We don't support marks.
	   */
	  @Override
	  public boolean markSupported() {
	    return false;
	  }

	  @Override
	  public void mark(int readLimit) {
	    // Do nothing
	  }

	  @Override
	  public void reset() throws IOException {
	    throw new IOException("Mark not supported");
	  }
}
