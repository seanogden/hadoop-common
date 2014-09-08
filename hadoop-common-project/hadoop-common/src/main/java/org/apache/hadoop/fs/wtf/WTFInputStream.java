package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import org.apache.hadoop.fs.FSInputStream;
import org.wtf.client.Client;
import org.wtf.client.WTFClientException;

public class WTFInputStream extends FSInputStream {
	Client c;
	long fd;
	int SEEK_SET = 0;
	int SEEK_CUR = 1;
	int SEEK_END = 2;
	
	public WTFInputStream(Client c, long fd) {
		this.fd = fd;
		this.c = c;
	}

	  @Override
	  public synchronized long getPos() throws IOException {
		  
		  long offset = c.lseek(fd, 0, SEEK_CUR);
		  
		  if (offset < 0)
		  {
			  throw new IOException(c.error_location() + ": " + c.error_message());
		  }

		  return offset;
	  }

	  @Override
	  public synchronized int available() throws IOException {
		  long cur = getPos();
		  
		  long offset = c.lseek(fd, 0, SEEK_END);
		  if (offset < 0)
		  {
			  throw new IOException(c.error_location() + ": " + c.error_message());
		  }
		  
		  seek(cur);
		  
		  return (int) (offset - cur);
	  }

	  @Override
	  public synchronized void seek(long targetPos) throws IOException {
		  
		  long offset = c.lseek(fd, targetPos, SEEK_SET);
		  
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
		    long[] length = {-1};
		    byte[] data = {0};
		    int offset = 0;

			try {
				Boolean ok = c.read(fd, data, offset, length);
				
			    if (!ok)
			    {
			    	throw new IOException("Error reading");
			    }
			    
			} catch (WTFClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			if (length[0] == 0)
				return -1;
		    
		    int ret = (int)data[0];
		    if (ret < 0)
		    	ret = ret + 256;
		    
		    System.err.println("read() returned " + data[0] + " => " + ret);
		    
		    return ret;
	  }

	  @Override
	  public synchronized int read(byte buf[], int off, int len) throws IOException {

		    long[] length = {-1};
		    try {
				Boolean ok = c.read(fd, buf, off, length);
			    
				if (!ok)
			    {
			    	throw new IOException("Error reading");
			    }
			} catch (WTFClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    if (length[0] == 0)
		    	return -1;
		    else
		    	return (int) length[0];
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
