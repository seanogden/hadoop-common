package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.fs.FSInputStream;
import org.wtf.client.Client;

public class WTFInputStream extends FSInputStream {
	Client c;
	long fd;
	int SEEK_CUR = 1;
	int SEEK_SET = 0;
	int SEEK_END = 2;
	
	public WTFInputStream(Client c, long fd) {
		this.fd = fd;
		this.c = c;
	}

	  @Override
	  public synchronized long getPos() throws IOException {
		  int[] status = {-1};
		  long offset = c.lseek(fd, BigInteger.ZERO, SEEK_CUR, status);
		  if (offset < 0)
		  {
			  throw new IOException(c.error_location() + ": " + c.error_message());
		  }
		  
		  return offset;
	  }

	  @Override
	  public synchronized int available() throws IOException {
		  long cur = getPos();
		  
		  int[] status = {-1};
		  
		  long length = c.lseek(fd, BigInteger.ZERO, SEEK_END, status);
		  if (length < 0)
		  {
			  throw new IOException(c.error_location() + ": " + c.error_message());
		  }
		  
		  seek(cur);
		  
		  return (int) (length - cur);
	  }

	  @Override
	  public synchronized void seek(long targetPos) throws IOException {
		  int[] status = {-1};
		  long offset = c.lseek(fd, BigInteger.valueOf(targetPos), SEEK_SET, status);
		  if (offset < 0)
		  {
			  throw new IOException(c.error_location() + ": " + c.error_message());
		  }
	  }

	  @Override
	  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
	    return false;
	  }

	  @Override
	  public synchronized int read() throws IOException {
			int[] status = {-1};
		    long[] data_sz = {1};
		    byte[] data = {0};
		    long reqid = c.read_sync(fd, data, data_sz, status);
		    if (reqid < 0)
		    {
		    	throw new IOException(c.error_location() + ": " + c.error_message());
		    }
		    
		    return (int) data[0];
	  }

	  @Override
	  public synchronized int read(byte buf[], int off, int len) throws IOException {
			int[] status = {-1};
		    long[] data_sz = {buf.length - off < len ? buf.length - off : len};
		    byte[] data = new byte[(int) data_sz[0]];
		    long reqid = c.read_sync(fd, data, data_sz, status);
		    if (reqid < 0)
		    {
		    	throw new IOException(c.error_location() + ": " + c.error_message());
		    }
		    
		    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(buf, off, (int) data_sz[0]);
		    bb.put(data, 0, (int) data_sz[0]);
		    return (int) data_sz[0];
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
