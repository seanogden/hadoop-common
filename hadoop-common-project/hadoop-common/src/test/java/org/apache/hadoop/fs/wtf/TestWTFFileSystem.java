package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.sun.security.ntlm.Client;

import junit.framework.TestCase;


public class TestWTFFileSystem extends TestCase {
	
	public void testInitialize() throws IOException {
	    initializationTest("wtf://a:b@c", "wtf://a:b@c");
	}

	private void initializationTest(String initializationUri, String expectedUri)
			throws IOException {

		WTFFileSystem fs = new WTFFileSystem();
		fs.initialize(URI.create(initializationUri), new Configuration());
		System.out.println(fs.getWorkingDirectory());
		assertEquals(URI.create(expectedUri), fs.getUri());
		FSDataOutputStream os = fs.create(new Path("/foo"));
		os.write("hello world".getBytes());
		os.close();
		
		byte[] buf = new byte["hello world".length()];
		FSDataInputStream is = fs.open(new Path("/foo"));
		is.read(buf);
		
		assertTrue(Arrays.equals(buf, "hello world".getBytes()));
		
		FileStatus fstat = fs.getFileStatus(new Path("/foo"));
		
		assertTrue(fstat.isFile());
		assertEquals(fstat.getLen(), "hello world".length());
		
		fs.mkdirs(new Path("/bar"));
		fs.mkdirs(new Path("/bar/baz"));
		FileStatus[] st = fs.listStatus(new Path("/bar"));
		assertEquals(st.length, 1);
		
		
		
		//fs.rename(new Path("/foo"), new Path("/bar"));
		//is = fs.open(new Path("wtf:///bar"));
		//buf = new byte["hello world".length()];
		//is.read(buf);
		
		//assertTrue(Arrays.equals(buf, "hello world".getBytes()));
		is.close();
		fs.close();
	}
}
