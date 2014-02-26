package org.apache.hadoop.fs.wtf;

//import java.io.IOException;
//import java.net.URI;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

//import org.apache.hadoop.conf.Configuration;

public class TestWTFFileSystem extends TestCase {
	
	public void testInitialize() throws IOException {
		initializationTest("wtf://foo", "wtf://foo");
	}

	private void initializationTest(String initializationUri, String expectedUri)
			throws IOException {

		WTFFileSystem fs = new WTFFileSystem();
		fs.initialize(URI.create(initializationUri), new Configuration());
		assertEquals(URI.create(expectedUri), fs.getUri());
		fs.close();
	}
}
