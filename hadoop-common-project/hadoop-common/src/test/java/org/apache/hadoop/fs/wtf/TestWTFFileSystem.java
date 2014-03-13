package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;


public class TestWTFFileSystem extends TestCase {
	
	public void testInitialize() throws IOException {
	    initializationTest("wtf://a:b@c", "wtf://a:b@c");
	    initializationTest("wtf://a:b@c/", "wtf://a:b@c");
	    initializationTest("wtf://a:b@c/path", "wtf://a:b@c");
	    initializationTest("wtf://a@c", "wtf://a@c");
	    initializationTest("wtf://a@c/", "wtf://a@c");
	    initializationTest("wtf://a@c/path", "wtf://a@c");
	    initializationTest("wtf://c", "wtf://c");
	    initializationTest("wtf://c/", "wtf://c");
	    initializationTest("wtf://c/path", "wtf://c");
	}

	private void initializationTest(String initializationUri, String expectedUri)
			throws IOException {

		WTFFileSystem fs = new WTFFileSystem();
		fs.initialize(URI.create(initializationUri), new Configuration());
		assertEquals(URI.create(expectedUri), fs.getUri());
		FSDataOutputStream os = fs.create(new Path("/foo"));
		os.write("hello world".getBytes());
		os.close();
		
		byte[] buf = new byte["hello world".length()];
		FSDataInputStream is = fs.open(new Path("/foo"));
		is.read(buf);
		
		assertTrue(Arrays.equals(buf, "hello world".getBytes()));
		
		fs.rename(new Path("/foo"), new Path("/bar"));
		is = fs.open(new Path("/bar"));
		buf = new byte["hello world".length()];
		is.read(buf);
		
		assertTrue(Arrays.equals(buf, "hello world".getBytes()));
		is.close();
		fs.close();
	}
}
