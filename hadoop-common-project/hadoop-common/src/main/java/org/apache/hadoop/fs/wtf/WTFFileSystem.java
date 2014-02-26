package org.apache.hadoop.fs.wtf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class WTFFileSystem extends FileSystem {

	@Override
	public String getScheme() {
		return "wtf";
	}
	
	@Override
	public URI getUri() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException,
			IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		// TODO Auto-generated method stub

	}

	@Override
	public Path getWorkingDirectory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
