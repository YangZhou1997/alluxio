/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.speedup;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.ConsistentUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

/**
 * Speedup FS {@link UnderFileSystem} implementation.
 * <p>
 * This is primarily intended for local unit testing and single machine mode. In principle, it can
 * also be used on a system where a shared file system (e.g. NFS) is mounted at the same path on
 * every node of the system. However, it is generally preferable to use a proper distributed file
 * system for that scenario.
 * </p>
 */
@ThreadSafe
public class SpeedupUnderFileSystem extends ConsistentUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  
	private static final Logger LOG = LoggerFactory.getLogger(SpeedupUnderFileSystem.class);

  /**
   * Constructs a new {@link SpeedupUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   */
	
  private InstancedConfiguration conf;
  private AlluxioURI alluxioBaseURI;
  private FileSystem underFS;
  private String base;
	
  public SpeedupUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
    // here, we will instantiate an alluxio client to connect
    String host = ufsConf.get(SpeedupUnderFileSystemPropertyKey.SPEEDUP_HOST_UFS_PROPERTY);
    int port = Integer.valueOf(ufsConf.get(SpeedupUnderFileSystemPropertyKey.SPEEDUP_PORT_UFS_PROPERTY));
    base = ufsConf.get(SpeedupUnderFileSystemPropertyKey.SPEEDUP_BASE_UFS_PROPERTY);
    
    LOG.info("Mounting uri=[{}] with host=[{}], port=[{}], base=[{}]", uri.toString(), host, port, base);
    
    // add the configuration
    conf = InstancedConfiguration.defaults();
    for(PropertyKey key : ufsConf.keySet()) {
    	conf.set(key, ufsConf.get(key));
    	LOG.info("Setting property [{}]=[{}]", key.toString(), ufsConf.get(key));
    }
    // also set the host
    conf.set(PropertyKey.MASTER_HOSTNAME, host);
    LOG.info("Setting property [{}]=[{}]", PropertyKey.MASTER_HOSTNAME, host);
    
    // now, we have the necessary configuration, lets set the base URI
    alluxioBaseURI = new AlluxioURI(uri.toString().replaceAll("speedup://", "alluxio://"));
    LOG.info("Base URI for communication will be [{}]", alluxioBaseURI.toString());
    
    // now we have to connect, creating a filesystem instance...
    LOG.info("Attempting connection to remote alluxio...");
    underFS = FileSystem.Factory.create(conf);
   
  }

  private String stripBase(String path) {
	  return path.replaceAll(base, "");
  }
  
  @Override
  public String getUnderFSType() {
    return "speedup";
  }

  @Override
  public void cleanup() throws IOException {
	  // this is a noop method
	  LOG.warn("Clean up called, nothing to do");
  }

  @Override
  public void close() throws IOException {
	  // Don't close; file systems are singletons and closing it here could break other users
	  LOG.warn("Close called, nothing to do");
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    path = stripBase(stripPath(path));
    // so here, i have the path of the file i am going to create
    AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
    try {
	    if(options.getCreateParent()) {
	    	// create the parent
	    	LOG.info("Creating parent directory [{}]", newURI.getParent());
	    	underFS.createDirectory(newURI.getParent());
	    }
	    LOG.info("Getting output stream for path: [{}]", newURI.toString());
	    return underFS.createFile(newURI);
    }
    catch(Exception e) {
    	LOG.error("Error when creating file", e);
    	throw new IOException(e);
    }
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
	path = stripBase(stripPath(path));
    // so here, i have the path of the file i am going to create
    AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
    try {
	    if(options.getCreateParent()) {
	    	// create the parent
	    	LOG.info("Creating parent directory [{}]", newURI.getParent());
	    	underFS.createDirectory(newURI.getParent());
	    }
	    LOG.info("Getting output stream for path: [{}]", newURI.toString());
	    return underFS.createFile(newURI);
    }
    catch(Exception e) {
    	LOG.error("Error when creating file", e);
    	throw new IOException(e);
    }
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
	path = stripBase(stripPath(path));
	AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	try {
		if(!underFS.exists(newURI)) {
			return false;
		}
		underFS.delete(newURI);
		return true;
	}
	catch(Exception e) {
		LOG.error("Error when deleting [{}]", newURI.toString());
		throw new IOException(e);
	}
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
	  path = stripBase(stripPath(path));
		AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
		try {
			if(!underFS.exists(newURI)) {
				return false;
			}
			underFS.delete(newURI);
			return true;
		}
		catch(Exception e) {
			LOG.error("Error when deleting [{}]", newURI.toString());
			throw new IOException(e);
		}
  }

  @Override
  public boolean exists(String path) throws IOException {
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		return underFS.exists(newURI);
	  }
	  catch(Exception e) {
		LOG.error("Error when checking file [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
	  // dont care
	  return 0L;
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    throw new UnsupportedOperationException("getDirectoryStatus is not supported");
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
	  throw new UnsupportedOperationException("getFileLocations is not supported");
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
	  throw new UnsupportedOperationException("getFileLocations is not supported");
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
	  throw new UnsupportedOperationException("getFileStatus is not supported");
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
	  throw new UnsupportedOperationException("getSpace is not supported");
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
	  throw new UnsupportedOperationException("getStatus is not supported");
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
	  throw new UnsupportedOperationException("isDirectory is not supported");
  }

  @Override
  public boolean isFile(String path) throws IOException {
	  throw new UnsupportedOperationException("isFile is not supported");
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
	  throw new UnsupportedOperationException("listStatus is not supported");
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
	  throw new UnsupportedOperationException("mkdirs is not supported");
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
	  path = stripBase(stripPath(path));
    // so here, i have the path of the file i am going to create
    AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
    try {
	    LOG.info("Getting output stream for path: [{}]", newURI.toString());
	    FileInStream inn = underFS.openFile(newURI);
	    inn.skip(options.getOffset());
	    return inn;
    }
    catch(Exception e) {
    	LOG.error("Error when creating file", e);
    	throw new IOException(e);
    }
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
	  throw new UnsupportedOperationException("renameDirectory is not supported");
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
	  throw new UnsupportedOperationException("renameFile is not supported");
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
	  throw new UnsupportedOperationException("setOwner is not supported");
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
	  throw new UnsupportedOperationException("setMode is not supported");
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    // No-op
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return true;
  }

  /**
   * Rename a file to a file or a directory to a directory.
   *
   * @param src path of source file or directory
   * @param dst path of destination file or directory
   * @return true if rename succeeds
   */
  private boolean rename(String src, String dst) throws IOException {
    src = stripPath(src);
    dst = stripPath(dst);
    File file = new File(src);
    return file.renameTo(new File(dst));
  }

  /**
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    return new AlluxioURI(path).getPath();
  }
}
