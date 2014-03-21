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
	    initializationTest("wtf://127.0.0.1:1981/", "wtf://127.0.0.1:1981");
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
		assertEquals(1, st.length);
		

		st = fs.listStatus(new Path("/"));
		assertEquals(2, st.length);
		
		st = fs.listStatus(new Path("wtf://127.0.0.1:1981/"));
		assertEquals(2, st.length);
		
		st = fs.listStatus(new Path("wtf://127.0.0.1:1981"));
		assertEquals(2, st.length);
		
		fs.create(new Path("/testoutput/_temporary/0/attempt_local363530570_0001_r_000000_0/part-r-00000"));
		
		fs.rename(new Path("/testoutput/_temporary/0/attempt_local363530570_0001_r_000000_0"), 
				new Path("/testoutput/_temporary/0/task_local705735145_0001_r_000000"));
		
		st = fs.listStatus(new Path("/testoutput/_temporary/0/task_local705735145_0001_r_000000"));
		assertEquals(1,st.length);
		//fs.rename(new Path("/foo"), new Path("/bar"));
		//is = fs.open(new Path("wtf:///bar"));
		//buf = new byte["hello world".length()];
		//is.read(buf);
		
		//assertTrue(Arrays.equals(buf, "hello world".getBytes()));
		is.close();
		fs.close();
	}
}
