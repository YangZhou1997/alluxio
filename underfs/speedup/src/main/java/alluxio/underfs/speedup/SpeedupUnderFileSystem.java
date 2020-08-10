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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
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
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.CreateFilePOptions;


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
    String host = ufsConf.get(PropertyKey.SPEEDUP_HOST_UFS_PROPERTY);
    int port = Integer.valueOf(ufsConf.get(PropertyKey.SPEEDUP_PORT_UFS_PROPERTY));
    base = ufsConf.get(PropertyKey.SPEEDUP_BASE_UFS_PROPERTY);
    
    LOG.info("Mounting uri=[{}] with host=[{}], port=[{}], base=[{}]", uri.toString(), host, port, base);
    
    // add the configuration
    conf = InstancedConfiguration.defaults();
    for(PropertyKey key : ufsConf.keySet()) {
    	try {
    		conf.set(key, ufsConf.get(key));
    		LOG.info("Setting property [{}]=[{}]", key.toString(), ufsConf.get(key));
    	}
    	catch(Exception e) {
    		LOG.warn("Missing value for configuration [{}]", key.toString());
    	}
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

  
  private OutputStream internalCreate(String path, CreateOptions options) throws IOException {
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
		    // pass the options too
		    CreateFilePOptions opts = CreateFilePOptions.newBuilder().setSpeedupFile(true).build();
		    return underFS.createFile(newURI, opts);
	    }
	    catch(Exception e) {
	    	LOG.error("Error when creating file", e);
	    	throw new IOException(e);
	    }
  }
  
  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return internalCreate(path, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
	  return internalCreate(path, options);
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
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		URIStatus status =  underFS.getStatus(newURI);
		LOG.info("Getting status for [{}]", newURI);
		UfsDirectoryStatus returnStatus = new UfsDirectoryStatus(status.getName(), status.getOwner(), status.getGroup(), (short)status.getMode(), status.getLastModificationTimeMs());
		return returnStatus;
	  }
	  catch(Exception e) {
		LOG.error("Error when checking status [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		LOG.info("Getting location for [{}]", newURI);
		List<URIStatus> status =  underFS.listStatus(newURI);
		List<String> names = new ArrayList<String>();
		for(URIStatus st : status) {
			names.add(st.getFileInfo().getName());
		}
		return names;
	  }
	  catch(Exception e) {
		LOG.error("Error when checking location [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
	  return getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		List<URIStatus> statuses = underFS.listStatus(newURI);
		if(statuses.size() >= 1) {
			UfsFileStatus returnStatus = new UfsFileStatus(statuses.get(0).getName(), 
					"", statuses.get(0).getLength(), statuses.get(0).getLastModificationTimeMs(), 
					statuses.get(0).getOwner(), statuses.get(0).getGroup(), (short)statuses.get(0).getMode());
			return returnStatus;
		}
		LOG.error("Could not establish status...");
		throw new IOException("Error when listing file status");
		
	  }
	  catch(Exception e) {
		LOG.error("Error when checking status [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
	  LOG.warn("Asking for free space, this is just a fixed constant here...");
	  return 1000000000L;
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		URIStatus stat = underFS.getStatus(newURI);
		return new UfsFileStatus(stat.getName(), 
				"", stat.getLength(), stat.getLastModificationTimeMs(), 
				stat.getOwner(), stat.getGroup(), (short)stat.getMode());
		
	  }
	  catch(Exception e) {
		LOG.error("Error when checking status [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
	path = stripBase(stripPath(path));
	AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	try {
		boolean ret =  underFS.exists(newURI); 
		LOG.info("isDirectory [{}]? {}", newURI.toString(), ret);
		return ret;
	}
	catch(Exception e) {
		LOG.error("Error when checking [{}]", newURI.toString());
		throw new IOException(e);
	}
  }

  @Override
  public boolean isFile(String path) throws IOException {
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		  boolean ret =  underFS.exists(newURI); 
		  LOG.info("isFile [{}]? {}", newURI.toString(), ret);
		  return ret;
	  }
	  catch(Exception e) {
		  LOG.error("Error when checking [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
	  path = stripBase(stripPath(path));
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
		  List<URIStatus> all = underFS.listStatus(newURI);
		  UfsStatus [] returnStatus = new UfsStatus[all.size()];
		  int i = 0;
		  for(URIStatus ss : all) {
			  returnStatus[i] = new UfsFileStatus(ss.getName(), 
						"", ss.getLength(), ss.getLastModificationTimeMs(), 
						ss.getOwner(), ss.getGroup(), (short)ss.getMode());
			  ++i;
		  }
		  return returnStatus;
	  }
	  catch(Exception e) {
		  LOG.error("Error when checking [{}]", newURI.toString());
		throw new IOException(e);
	  }
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
	  path = stripBase(stripPath(path));
	  // so here, i have the path of the file i am going to create
	  AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
	  try {
	    underFS.createDirectory(newURI);
	    return true;
	  }
	  catch(Exception e) {
    	LOG.error("Error when mkdirs", e);
    	return false;
	  }
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
    	LOG.error("Error when opening file", e);
    	throw new IOException(e);
    }
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    src = stripBase(stripPath(src));
    dst = stripBase(stripPath(dst));
    // so here, i have the path of the file i am going to create
    AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + src);
    AlluxioURI dURI = new AlluxioURI(alluxioBaseURI.toString() + dst);
    try {
	    LOG.info("Renaming [{}] to [{}]", newURI.toString(), dURI.toString());
	    underFS.rename(newURI, dURI);
	    return true;
    }
    catch(Exception e) {
    	LOG.error("Error when renaming dir", e);
    	return false;
    }
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
	src = stripBase(stripPath(src));
    dst = stripBase(stripPath(dst));
    // so here, i have the path of the file i am going to create
    AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + src);
    AlluxioURI dURI = new AlluxioURI(alluxioBaseURI.toString() + dst);
    try {
	    LOG.info("Renaming [{}] to [{}]", newURI.toString(), dURI.toString());
	    underFS.rename(newURI, dURI);
	    return true;
    }
    catch(Exception e) {
    	LOG.error("Error when renaming file", e);
    	return false;
    }
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
	path = stripBase(stripPath(path));
    // so here, i have the path of the file i am going to create
    AlluxioURI newURI = new AlluxioURI(alluxioBaseURI.toString() + path);
    try {
	    LOG.info("Setting owner for: [{}]", newURI.toString());
	    SetAttributePOptions opts = SetAttributePOptions.newBuilder().setOwner(user).setGroup(group).build();
	    underFS.setAttribute(newURI, opts);
    }
    catch(Exception e) {
    	LOG.error("Error when creating file", e);
    	throw new IOException(e);
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
	// not sure how to implement this, so it will be a noop
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
