package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.wtf.client.Client;

public class WTFOutputStream extends OutputStream {

	private long fd;
	private Client c;
	
	public WTFOutputStream(Client client, long fd) {
		this.fd = fd;
		this.c = client;
	}

	@Override
	public synchronized void write(int b) throws IOException {
		int[] status = {-1};
	    long[] data_sz = {1};
	   	byte[] data = {(byte) b};

	   	//System.out.println("Writing " + new String(data));
	   	
	    long reqid = c.write_sync(fd, data, data_sz, 3, status);
	    if (reqid < 0)
	    {
	    	throw new IOException(c.error_location() + ": " + c.error_message());
	    }
	}

	@Override
	public synchronized void write(byte b[], int off, int len) throws IOException {
		int[] status = {-1};
	    long[] data_sz = {b.length - off < len ? b.length - off : len};
	   	byte[] data = ByteBuffer.wrap(b, off, (int) data_sz[0]).array();
	   	
	   	//System.out.println("Writing " + new String(data));
	    //System.out.println("b.length = " + b.length + " off = " + off + " len = " + len);
	   	long reqid = c.write_sync(fd, data, data_sz, 3, status);
	    if (reqid < 0)
	    {
	    	throw new IOException(c.error_location() + ": " + c.error_message());
	    }
	}

	@Override
	public synchronized void flush() throws IOException {
		//our ops are synchronous, so flush is already done.
		return;
	}

	@Override
	public synchronized void close() throws IOException {
		int[] status = {-1};
		long reqid = c.close(fd, status);
		if (reqid < 0)
		{
			throw new IOException(c.error_location() + ": " + c.error_message());
		}
	}

}
