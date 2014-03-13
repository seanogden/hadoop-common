package org.apache.hadoop.fs.wtf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.wtf.client.Client;


public class WTFFileSystem extends FileSystem {

	private URI uri;
	private Client client;
	private static int O_RDONLY = 0000;
	@SuppressWarnings("unused")
	private static int O_RDWR   = 0002;
	private static int O_WRONLY = 0001;
	private static int O_CREAT  = 0100;
	
	@Override
	public String getScheme() {
		return "wtf";
	}
	
	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
		
		this.client = new Client(
				conf.get(WTFConfigKeys.WTF_COORDINATOR_HOST_KEY,
						 WTFConfigKeys.WTF_COORDINATOR_HOST_DEFAULT),
				conf.getInt(WTFConfigKeys.WTF_COORDINATOR_PORT_KEY, 
							WTFConfigKeys.WTF_COORDINATOR_PORT_DEFAULT),
			    conf.get(WTFConfigKeys.WTF_HYPERDEX_HOST_KEY,
			    		 WTFConfigKeys.WTF_HYPERDEX_HOST_DEFAULT),
				conf.getInt(WTFConfigKeys.WTF_HYPERDEX_PORT_KEY, 
							WTFConfigKeys.WTF_HYPERDEX_PORT_DEFAULT));
	}  

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		int[] status = {-1};
		long fd = client.open(f.toString(), O_RDONLY, 0, 0, 0, status);
		if (fd < 0) {
			throw new IOException(client.error_location() + ": " + client.error_message());
		}
		return new FSDataInputStream(new WTFInputStream(client, fd));
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		
		int[] status = {-1};
		long fd = client.open(f.toString(), O_WRONLY | O_CREAT, 
							  0777, replication, blockSize, status);
	    if (fd < 0) {
	    	throw new IOException(client.error_location() + ": " + client.error_message());
	    }
	    
	    return new FSDataOutputStream(new WTFOutputStream(client, fd), statistics);
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		int[] status = {-1};
		long ret = client.rename(src.getName(), dst.getName(), status);
		if (ret < 0)
			throw new IOException(client.error_location() + ": " + client.error_message());
		return true;
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		int[] status = {-1};
		long ret = client.unlink(f.getName(), status);
		if (ret < 0)
			return false;
		return true;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		int[] status = {-1};
		client.chdir(new_dir.toString(), status);
	}

	@Override
	public Path getWorkingDirectory() {
		//TODO: figure out how to make this output a string.
		String c = null;
		int[] status = {-1};
		client.getcwd(c, 255, status);
		return new Path(c);
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		int[] status = {-1};
		long reqid = client.mkdir(f.toString(), FsPermission.getDirDefault().toShort(), status);
		if (reqid < 0) {
			throw new IOException(client.error_location() + ": " + client.error_message());
		}
		
		return true;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		return new FileStatus();
	}

}
