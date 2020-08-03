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

package alluxio.underfs.alluxio;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link AlluxioUnderFileSystem}.
 *
 * It caches created {@link AlluxioUnderFileSystem}s, using the scheme and authority pair as the key.
 */
@ThreadSafe
public final class AlluxioUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link AlluxioUnderFileSystemFactory}.
   */
  public AlluxioUnderFileSystemFactory() { }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    return AlluxioUnderFileSystem.createInstance(new AlluxioURI(path), conf);
  }

  @Override
  public boolean supportsPath(String path) {
    if (path != null) {
      // we extending hdfs ufs to support alluxio fs: 
      // as alluxio.hadoop.FileSystem provides a Hadoop compatible FileSystem interface; 
      if (path.startsWith("alluxio://")){
          return true;
      }
    }
    return false;
  }

  @Override
  public boolean supportsPath(String path, UnderFileSystemConfiguration conf) {
    if (path != null) {
      // we extending hdfs ufs to support alluxio fs: 
      // as alluxio.hadoop.FileSystem provides a Hadoop compatible FileSystem interface; 
      if (path.startsWith("alluxio://")){
        return true;
      }
    }
    return false;
  }

  @Override
  public String getVersion() {
    return alluxio.UfsConstants.UFS_HADOOP_VERSION;
  }
}
