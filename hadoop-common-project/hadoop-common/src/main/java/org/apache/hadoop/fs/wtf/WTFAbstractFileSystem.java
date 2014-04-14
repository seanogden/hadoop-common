package org.apache.hadoop.fs.wtf;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.util.DataChecksum;

public class WTFAbstractFileSystem extends DelegateToFileSystem {
	  
	WTFAbstractFileSystem(final Configuration conf) throws IOException, URISyntaxException {
		    this(new URI("wtf://" + conf.get(WTFConfigKeys.WTF_COORDINATOR_HOST_KEY,
					 WTFConfigKeys.WTF_COORDINATOR_HOST_DEFAULT) + ":" +
					 conf.getInt(WTFConfigKeys.WTF_COORDINATOR_PORT_KEY,
							 WTFConfigKeys.WTF_COORDINATOR_PORT_DEFAULT))
		    	, conf);
		  }
		  
		  /**
		   * This constructor has the signature needed by
		   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
		   * 
		   * @param theUri which must be that of localFs
		   * @param conf
		   * @throws IOException
		   * @throws URISyntaxException 
		   */
	WTFAbstractFileSystem(final URI theUri, final Configuration conf) throws IOException,
		      URISyntaxException {
		    super(theUri, new WTFFileSystem(theUri, conf), conf, 
		        theUri.getScheme(), false);
		  }
	
	@Override
	public FsServerDefaults getServerDefaults() throws IOException {
		    return new FsServerDefaults(
		        WTFConfigKeys.WTF_BLOCK_SIZE_DEFAULT,
		        512,
		        WTFConfigKeys.WTF_CLIENT_WRITE_PACKET_SIZE_DEFAULT,
		        (short) WTFConfigKeys.WTF_REPLICATION_DEFAULT,
		        WTFConfigKeys.WTF_STREAM_BUFFER_SIZE_DEFAULT,
		        WTFConfigKeys.WTF_ENCRYPT_DATA_TRANSFER_DEFAULT,
		        0,
		        DataChecksum.Type.NULL);
	}
	
	  @Override
	  public boolean isValidName(String src) {
	    // Different local file systems have different validation rules. Skip
	    // validation here and just let the OS handle it. This is consistent with
	    // RawLocalFileSystem.
	    return true;
	  }
	  
	  @Override
	  public int getUriDefaultPort() {
	    return WTFConfigKeys.WTF_COORDINATOR_PORT_DEFAULT; // No default port for file:///
	  }


}
