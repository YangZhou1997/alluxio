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

package alluxio.client.block.stream;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DataMessage;
import alluxio.grpc.ReadHashRequest;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;
import vmware.speedup.chunk.Chunk;
import vmware.speedup.chunk.Hash;
import vmware.speedup.common.HashingException;
import vmware.speedup.chunk.CassandraChunkLocation;
import vmware.speedup.chunk.CassandraHash;
import com.google.protobuf.ByteString;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data reader that streams a region from gRPC data server.
 *
 * Protocol:
 * 1. The client sends a read request (id, offset, length).
 * 2. Once the server receives the request, it streams chunks to the client. The streaming pauses
 *    if the server's buffer is full and resumes if the buffer is not full.
 * 3. The client reads chunks from the stream using an iterator.
 * 4. The client can cancel the read request at anytime. The cancel request is ignored by the
 *    server if everything has been sent to channel.
 * 5. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@NotThreadSafe
public final class GrpcDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataReader.class);

  private final int mReaderBufferSizeMessages;
  private final long mDataTimeoutMs;
  private final FileSystemContext mContext;
  private final CloseableResource<BlockWorkerClient> mClient;
  private final ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;
  private final ReadResponseMarshaller mMarshaller;

  /** The next pos to read. */
  private long mPosToRead;

  private static final String DUMMY_HOST = "dummy";

  /**
   * Creates an instance of {@link GrpcDataReader}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param readRequest the read request
   */
  private GrpcDataReader(FileSystemContext context, WorkerNetAddress address,
      ReadRequest readRequest) throws IOException {
    mContext = context;
    mAddress = address;
    mPosToRead = readRequest.getOffset();
    mReadRequest = readRequest;
    AlluxioConfiguration alluxioConf = context.getClusterConf();
    mReaderBufferSizeMessages = alluxioConf
        .getInt(PropertyKey.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES);
    mDataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_STREAMING_DATA_TIMEOUT);
    mMarshaller = new ReadResponseMarshaller();
    mClient = mContext.acquireBlockWorkerClient(address);

    try {
      if (alluxioConf.getBoolean(PropertyKey.USER_STREAMING_ZEROCOPY_ENABLED)) {
        String desc = "Zero Copy GrpcDataReader";
        if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
          desc = MoreObjects.toStringHelper(this)
              .add("request", mReadRequest)
              .add("address", address)
              .toString();
        }
        mStream = new GrpcDataMessageBlockingStream<>(mClient.get()::readBlock,
            mReaderBufferSizeMessages,
            desc, null, mMarshaller);
      } else {
        String desc = "GrpcDataReader";
        if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
          desc = MoreObjects.toStringHelper(this)
              .add("request", mReadRequest)
              .add("address", address)
              .toString();
        }
        mStream = new GrpcBlockingStream<>(mClient.get()::readBlock, mReaderBufferSizeMessages,
            desc);
      }
      // @yang, this is the first time sending the read request, which will trigger 
      // a DataReader thread at AbstractReadHandler. 
      mStream.send(mReadRequest, mDataTimeoutMs);
    } catch (Exception e) {
      mClient.close();
      throw e;
    }
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

   /**
   * Handles write request.
   *
   * @param writeRequest the request from the client
   */
  
    private byte[] handleWriteHashRequest(byte[] query) {
        // @yang: query
        LOG.info("@cesar: will query [{}] to cassie", Bytes.toHexString(query));
        try {
            List<CassandraChunkLocation> result = DefaultBlockWorker.chunkStore.getChunkByIdentifier(CassandraHash.fromHash(query, DUMMY_HOST));
            int ack = result == null || result.size() == 0? -1 : 1;
            mResponseObserver.onNext(
                    WriteResponse.newBuilder().setOffset(ack).build());
            if(result != null && result.size() == 1) {
                // here we need to get the data
                CassandraChunkLocation chunk = result.get(0);
                String path = "/mnt/ramdisk/alluxioworker/" + chunk.getBlockId();
                LOG.info("@cesar: Going to read [{}] bytes from [{}] at offset {{}}", chunk.getLen(), path, chunk.getOffset());
                RandomAccessFile rand = new RandomAccessFile(path, "r");
                rand.seek(chunk.getOffset());
                byte[] content = new byte[chunk.getLen()];
                rand.read(content, 0, content.length);
                rand.close();
                return content;
            }
            else {
                LOG.info("@cesar: Received a result with [{}] rows", result.size());
            }
            return null;
        }
        catch(Exception e) {
            LOG.error("@cesar: Exception when retrieving chunk!", e);
            return null;
        }
    }

    private byte[] handleDedupStore(byte[] content, long blockId, String workerHost, long offset) {
        // @yang: store
        try {
            CassandraChunkLocation chunk = CassandraChunkLocation.build(
                    DefaultBlockWorker.chunkStore.getHashWorkers().hashContent(content), 
                    DUMMY_HOST, String.valueOf(blockId), (int)offset, content.length);
            DefaultBlockWorker.chunkStore.storeChunk(Lists.newArrayList(chunk));
            return chunk.getHash();
        }
        catch(Exception e) {
            LOG.error("Exception when hashing", e);
            return null;
        }
        
    }

  @Override
  public DataBuffer readChunk() throws IOException {
    Preconditions.checkState(!mClient.get().isShutdown(),
        "Data reader is closed while reading data chunks.");
    DataBuffer buffer = null;
    ReadResponse response = null;
    // @yang, generally, we need to enter a loop to receive multiple hash query, and answer 
    // ack or nack, based on the query results from DefaultBlockWorker.chunkStore. 
    // 
    while(true){
        if (mStream instanceof GrpcDataMessageBlockingStream) {
          DataMessage<ReadResponse, DataBuffer> message =
              ((GrpcDataMessageBlockingStream<ReadRequest, ReadResponse>) mStream)
                  .receiveDataMessage(mDataTimeoutMs);
          if (message != null) {
            response = message.getMessage();
            buffer = message.getBuffer();
            if (buffer == null && response.hasChunk() && response.getChunk().hasData()) {
              // falls back to use chunk message for compatibility
              ByteBuffer byteBuffer = response.getChunk().getData().asReadOnlyByteBuffer();
              buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
            }
            Preconditions.checkState(buffer != null, "response should always contain chunk");
          }
        } else {
          response = mStream.receive(mDataTimeoutMs);
          if (response != null) {
            Preconditions.checkState(response.hasChunk() && response.getChunk().hasData(),
                "response should always contain chunk");
            ByteBuffer byteBuffer = response.getChunk().getData().asReadOnlyByteBuffer();
            buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
          }
        }
        if (response == null) {
          return null;
        }
        if(response.hasReadHashRequest()){
            // @yang, we recevied a ReadHashRequest. 
            LOG.info("Receiving signature request...");
            // reply if we dont have it...
            ReadHashRequest hashQuery = response.getReadHashRequest();
            LOG.info("@yang: Received a query for [{}]", hashQuery.getHash());

        	byte[] content = handleWriteHashRequest(hashQuery.getHash().toByteArray());
            // so now, we have to handle the response
        	if(content != null) {
        		ByteString data = ByteString.copyFrom(content);
        		writeData(new NioDataBuffer(data.asReadOnlyByteBuffer(), data.size()));
        		LOG.info("Writing {} bytes", data.size());
        	}
        	// if content is null, the a normal write request will come...
        }
    	if(response.hasChunk() && response.getChunk().getDedup()) {
            mPosToRead += buffer.readableBytes();
            byte[] data = new byte[(int)buffer.getLength()];
	        buffer.readBytes(data, 0, data.length);
	        byte[] content = handleDedupStore(data);
	        LOG.info("@yang: Stored [{}]", Bytes.toHexString(content));
        }
        else{
            mPosToRead += buffer.readableBytes();
        }
        if(not reading all bytes){
            continue;
        }
        try {
          mStream.send(mReadRequest.toBuilder().setOffsetReceived(mPosToRead).build());
        } catch (Exception e) {
          // nothing is done as the receipt is sent at best effort
          LOG.debug("Failed to send receipt of data to worker {} for request {}: {}.", mAddress,
              mReadRequest, e.getMessage());
        }
        Preconditions.checkState(mPosToRead - mReadRequest.getOffset() <= mReadRequest.getLength());
    }
    return buffer;
  }

  @Override
  public void close() throws IOException {
    try {
      if (mClient.get().isShutdown()) {
        return;
      }
      mStream.close();
      mStream.waitForComplete(mDataTimeoutMs);
    } finally {
      mMarshaller.close();
      mClient.close();
    }
  }

  /**
   * Factory class to create {@link GrpcDataReader}s.
   */
  public static class Factory implements DataReader.Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final ReadRequest mReadRequestPartial;

    /**
     * Creates an instance of {@link GrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestPartial the partial read request
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest readRequestPartial) {
      mContext = context;
      mAddress = address;
      mReadRequestPartial = readRequestPartial;
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      return new GrpcDataReader(mContext, mAddress,
          mReadRequestPartial.toBuilder().setOffset(offset).setLength(len).build());
    }

    @Override
    public boolean isShortCircuit() {
      return false;
    }

    @Override
    public void close() throws IOException {}
  }
}

