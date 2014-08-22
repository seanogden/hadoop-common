package org.apache.hadoop.fs.wtf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.wtf.client.Client;
import org.wtf.client.Iterator;
import org.wtf.client.WTFClientException;
import org.wtf.client.WTFFileAttrs;


public class WTFFileSystem extends FileSystem {

	private URI uri;
	private Client client;
	private static int O_RDONLY = 0000;
	@SuppressWarnings("unused")
	private static int O_RDWR   = 0002;
	private static int O_WRONLY = 0001;
	private static int O_CREAT  = 0100;
	private Path workingDir;
	
	@Override
	public String getScheme() {
		return "wtf";
	}
	
	@Override
	public URI getUri() {
		return uri;
	}

	public WTFFileSystem()
	{
	}
	
	public WTFFileSystem(final URI uri, final Configuration conf) throws IOException
	{
		initialize(uri, conf);
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
		
		String host = uri.getHost();
		int port = uri.getPort();
		
		if (host.equals(""))
			host = conf.get(WTFConfigKeys.WTF_COORDINATOR_HOST_KEY,
					 WTFConfigKeys.WTF_COORDINATOR_HOST_DEFAULT);
		
		if (port == 0)
			port = conf.getInt(WTFConfigKeys.WTF_COORDINATOR_PORT_KEY, 
					WTFConfigKeys.WTF_COORDINATOR_PORT_DEFAULT);
			
		this.client = new Client(
				host,
				port,
			    conf.get(WTFConfigKeys.WTF_HYPERDEX_HOST_KEY,
			    		 WTFConfigKeys.WTF_HYPERDEX_HOST_DEFAULT),
				conf.getInt(WTFConfigKeys.WTF_HYPERDEX_PORT_KEY, 
							WTFConfigKeys.WTF_HYPERDEX_PORT_DEFAULT));
		this.workingDir =
			      new Path("/user", System.getProperty("user.name")).makeQualified(this);
	}  

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		long[] fd = {-1};
		Boolean ok = false;
		try {
			ok = client.open(f.toUri().getPath(), O_RDONLY, 0, 0, 
								  (int) WTFConfigKeys.WTF_BLOCK_SIZE_DEFAULT, fd);
		} catch (WTFClientException e) {
			
			if (!ok)
			{
				throw new IOException(client.error_location() + ": " + client.error_message());
			}
			
			e.printStackTrace();
		}

		
		return new FSDataInputStream(new WTFInputStream(client, fd[0]));
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		mkdirs(f.getParent());
		long[] fd = {-1};
		Boolean ok;
		try {
			ok = client.open(f.toUri().getPath(), 
								  O_WRONLY | O_CREAT, 
								  0777, 
								  (int) WTFConfigKeys.WTF_REPLICATION_DEFAULT, 
								  (int)WTFConfigKeys.WTF_BLOCK_SIZE_DEFAULT,
								  fd);
		} catch (WTFClientException e) {
			e.printStackTrace();
			throw new IOException(client.error_location() + ": " + client.error_message());
		}

	    
	    return new FSDataOutputStream(new WTFOutputStream(client, fd[0], getConf(), blockSize, bufferSize), statistics);
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {

		for(FileStatus st : listStatus(src))
		{
			rename(st.getPath(),
					new Path(st.getPath().toUri().getPath().replace(src.toUri().getPath(), dst.toUri().getPath())));
		}
		
		try {
			if (!client.rename(src.toUri().getPath(), dst.toUri().getPath()))
				throw new IOException(client.error_location() + ": " + client.error_message());
		} catch (WTFClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		
		Boolean ok = client.unlink(f.toUri().getPath());
		return ok;
	}
	
	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		
		f = fixRelativePart(f);
		if (f.toUri().getPath().equals(""))
		{
			f = new Path("/");
		}
		
		Iterator it = client.readdir(f.toUri().getPath());
		
		List<FileStatus> fstats = new ArrayList<FileStatus>();
		
		while (it.hasNext())
		{
			Path fname = new Path((String)it.next());
			
			if (fname.toUri().getPath().equals(f.toUri().getPath()))
			{
				continue;
			}
			
			if (fname.isRoot())
			{
				continue;
			}
			
			if (!fname.getParent().toUri().getPath().equals(f.toUri().getPath()))
			{
				continue;
			}
			
			WTFFileAttrs fa = new WTFFileAttrs();
			try {
				client.getattr(fname.toUri().getPath(), fa);
			} catch (WTFClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			fstats.add(new WTFFileStatus(fname, fa));
		}
		
		FileStatus[] ret = fstats.toArray(new FileStatus[fstats.size()]);
		
		return ret;
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		this.workingDir = new_dir;
	}

	@Override
	public Path getWorkingDirectory() {
		return this.workingDir;
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		f = fixRelativePart(f);
		while (!f.isRoot()){
			Boolean ok = client.mkdir(f.toUri().getPath(), FsPermission.getDirDefault().toShort());		
			f = f.getParent();
		}
		return true;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		
		WTFFileAttrs fa = new WTFFileAttrs();
		
	    try {
			if (!client.getattr(f.toUri().getPath(), fa)) {
			    throw new FileNotFoundException(f + ": No such file or directory.");
			}
		} catch (WTFClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("size = " + fa.sz);
		
		return new WTFFileStatus(f, fa);
	}
	
	private static class WTFFileStatus extends FileStatus {
		WTFFileStatus(Path f, WTFFileAttrs fa) throws IOException {
			super(fa.sz, fa.isDir==1, 
					(int) WTFConfigKeys.WTF_REPLICATION_DEFAULT, 
					  WTFConfigKeys.WTF_BLOCK_SIZE_DEFAULT*1024, 0, f);
		}
	}

}
