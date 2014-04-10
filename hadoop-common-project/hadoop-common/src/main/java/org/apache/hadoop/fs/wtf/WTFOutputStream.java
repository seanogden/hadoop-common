package org.apache.hadoop.fs.wtf;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.wtf.client.Client;

public class WTFOutputStream extends OutputStream {
	private Configuration conf;
	private int bufferSize;
	private long blockSize;
	private File backupFile;
	private OutputStream backupStream;
	private boolean closed;
	private int pos = 0;
	private long filePos = 0;
	private int bytesWrittenToBlock = 0;
	private byte[] outBuf;
	private long fd;
	private Client c;

	public WTFOutputStream(Client client, long fd, Configuration conf,
			long blocksize, int buffersize) throws IOException {
		this.fd = fd;
		this.c = client;
		this.conf = conf;
		this.blockSize = blocksize;
		this.backupFile = newBackupFile();
		this.backupStream = new FileOutputStream(backupFile);
		this.bufferSize = buffersize;
		this.outBuf = new byte[buffersize];
	}

	  @Override
	  public synchronized void write(int b) throws IOException {
	    if (closed) {
	      throw new IOException("Stream closed");
	    }

	    if ((bytesWrittenToBlock + pos == blockSize) || (pos >= bufferSize)) {
	      flush();
	    }
	    outBuf[pos++] = (byte) b;
	    filePos++;
	  }
	
	  @Override
	  public synchronized void write(byte b[], int off, int len) throws IOException {
	    if (closed) {
	      throw new IOException("Stream closed");
	    }
	    while (len > 0) {
	      int remaining = bufferSize - pos;
	      int toWrite = Math.min(remaining, len);
	      System.arraycopy(b, off, outBuf, pos, toWrite);
	      pos += toWrite;
	      off += toWrite;
	      len -= toWrite;
	      filePos += toWrite;

	      if ((bytesWrittenToBlock + pos >= blockSize) || (pos == bufferSize)) {
	        flush();
	      }
	    }
	  }

	private void storeBlock() throws IOException {
		BufferedInputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(backupFile));
			byte[] data = IOUtils.toByteArray(in);
			long[] data_sz = {data.length};
			int[] status = {-1};
			
			System.out.println("Writing " + data_sz[0] + " bytes to WTF");
			long reqid = c.write_sync(fd, data, data_sz, 3, status);
			if (reqid < 0)
			{
				throw new IOException(c.error_location() + ": " + c.error_message());
			}

			System.out.println("Done.");
			
		} finally {
			closeQuietly(in);
		}    
	}

	private void closeQuietly(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e) {
				// ignore
			}
		}
	}
	private File newBackupFile() throws IOException {
		File dir = new File(conf.get(WTFConfigKeys.WTF_BUFFER_DIR_KEY, WTFConfigKeys.WTF_BUFFER_DIR_DEFAULT));
		if (!dir.exists() && !dir.mkdirs()) {
			throw new IOException("Cannot create wtf buffer directory: " + dir);
		}
		File result = File.createTempFile("output-", ".tmp", dir);
		result.deleteOnExit();
		return result;
	}
	/*
	 * This is where we actually write the block to WTF.
	 */
	private synchronized void endBlock() throws IOException {
		// Done with local copy
		backupStream.close();

		// Send it to WTF
		storeBlock();

		// Delete local backup, start new one
		backupFile.delete();
		backupFile = newBackupFile();
		backupStream = new FileOutputStream(backupFile);
		bytesWrittenToBlock = 0;
	}

	public long getPos() throws IOException {
		return filePos;
	}

	@Override
	public synchronized void flush() throws IOException {
		if (closed) {
			throw new IOException("Stream closed");
		}

		if (bytesWrittenToBlock + pos >= blockSize) {
			flushData((int) blockSize - bytesWrittenToBlock);
		}
		if (bytesWrittenToBlock == blockSize) {
			endBlock();
		}
		flushData(pos);
	}

	private synchronized void flushData(int maxPos) throws IOException {
		int workingPos = Math.min(pos, maxPos);

		if (workingPos > 0) {
			// To the local block backup, write just the bytes
			backupStream.write(outBuf, 0, workingPos);

			// Track position
			bytesWrittenToBlock += workingPos;
			System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
			pos -= workingPos;
		}
	}
	  @Override
	  public synchronized void close() throws IOException {
	    if (closed) {
	      return;
	    }

	    flush();
	    if (filePos == 0 || bytesWrittenToBlock != 0) {
	      endBlock();
	    }

	    backupStream.close();
	    internal_close();
	    backupFile.delete();
	    super.close();
	    closed = true;
	  }

	private synchronized void internal_close() throws IOException {
		int[] status = {-1};
		long reqid = c.close(fd, status);
		if (reqid < 0)
		{
			throw new IOException(c.error_location() + ": " + c.error_message());
		}
	}

}
