package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
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
		fs.close();
	}
}
