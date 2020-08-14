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

package alluxio.grpc;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.WireFormat;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Marshaller for {@link WriteRequest}.
 */
@NotThreadSafe
public class WriteRequestMarshaller extends DataMessageMarshaller<WriteRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(WriteRequestMarshaller.class);	
	
  private static final int CHUNK_TAG = GrpcSerializationUtils.makeTag(
      WriteRequest.CHUNK_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  
  private static final int HASH_TAG = GrpcSerializationUtils.makeTag(
	      WriteRequest.WRITEHASHREQUEST_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  
  
  /**
   * Creates a {@link WriteRequestMarshaller}.
   */
  public WriteRequestMarshaller() {
    super(BlockWorkerGrpc.getWriteBlockMethod().getRequestMarshaller());
  }

  @Override
  protected ByteBuf[] serialize(WriteRequest message) throws IOException {
    if (message.hasCommand()) {
      byte[] command = new byte[message.getSerializedSize()];
      CodedOutputStream stream = CodedOutputStream.newInstance(command);
      message.writeTo(stream);
      return new ByteBuf[] { Unpooled.wrappedBuffer(command) };
    }
    // @cesar: Need to handle this too
    else if(message.hasWriteHashRequest()) {
    	byte[] hash = new byte[message.getSerializedSize()];
        CodedOutputStream stream = CodedOutputStream.newInstance(hash);
        message.writeTo(stream);
        return new ByteBuf[] { Unpooled.wrappedBuffer(hash) };
    }
    DataBuffer chunkBuffer = pollBuffer(message);
    if (chunkBuffer == null) {
      if (!message.hasChunk() || !message.getChunk().hasData()) {
        // nothing to serialize
        return new ByteBuf[0];
      }
      // attempts to fallback to read chunk from message
      chunkBuffer = new NettyDataBuffer(
          Unpooled.wrappedBuffer(message.getChunk().getData().asReadOnlyByteBuffer()));
    }
    int headerSize = message.getSerializedSize() - chunkBuffer.readableBytes();
    byte[] header = new byte[headerSize];
    CodedOutputStream stream = CodedOutputStream.newInstance(header);
    stream.writeTag(WriteRequest.CHUNK_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    stream.writeUInt32NoTag(message.getChunk().getSerializedSize());
    stream.writeTag(Chunk.DEDUP_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    stream.writeUInt32NoTag(message.getChunk().getDedup()? 1: 0);
    stream.writeTag(Chunk.DATA_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    stream.writeUInt32NoTag(chunkBuffer.readableBytes());
    
    LOG.info("@cesar: Serializing a message with dedup: {}", message.getChunk().getDedup());
    
    return new ByteBuf[] { Unpooled.wrappedBuffer(header), (ByteBuf) chunkBuffer.getNettyOutput() };
  }

  @Override
  protected WriteRequest deserialize(ReadableBuffer buffer) throws IOException {
    if (buffer.readableBytes() == 0) {
      return WriteRequest.getDefaultInstance();
    }
    try (InputStream is = ReadableBuffers.openStream(buffer, false)) {
      int tag = ProtoUtils.readRawVarint32(is);
      int messageSize = ProtoUtils.readRawVarint32(is);
      if (tag != CHUNK_TAG) {
    	// @cesar: I had to modify this here too
    	if(tag != HASH_TAG) {  
    		return WriteRequest.newBuilder().setCommand(WriteRequestCommand.parseFrom(is)).build();
    	}
    	else {
    		return WriteRequest.newBuilder().setWriteHashRequest(WriteHashRequest.parseFrom(is)).build();
    	}
      }
      // @cesar: After this, read the dedup tag
      int dedupTag = ProtoUtils.readRawVarint32(is);
      int dedup = ProtoUtils.readRawVarint32(is);
      // @cesar: Ill comment this check, is not true anymore
      // Preconditions.checkState(messageSize == buffer.readableBytes());
      Preconditions.checkState(ProtoUtils.readRawVarint32(is) == GrpcSerializationUtils.makeTag(
          Chunk.DATA_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED));
      int chunkSize = ProtoUtils.readRawVarint32(is);
      // @cesar: Ill comment this one out too
      Preconditions.checkState(chunkSize == buffer.readableBytes());
      WriteRequest request = WriteRequest.newBuilder().setChunk(
    		  Chunk.newBuilder().setDedup(dedup > 0? true : false)).build();
      // @cesar: and set this value here
      ByteBuf bytebuf = GrpcSerializationUtils.getByteBufFromReadableBuffer(buffer);
      if (bytebuf != null) {
        offerBuffer(new NettyDataBuffer(bytebuf), request);
      } else {
        offerBuffer(new ReadableDataBuffer(buffer), request);
      }
      
      LOG.info("@cesar: Deserializing a message with dedup: {}", dedup);
      
      return request;
    }
  }

  @Override
  public WriteRequest combineData(DataMessage<WriteRequest, DataBuffer> message) {
    if (message == null) {
      return null;
    }
    DataBuffer buffer = message.getBuffer();
    if (buffer == null) {
      return message.getMessage();
    }
    try {
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes, 0, bytes.length);
      // @cesar: I need this routing here...
      boolean dedup = false;
      if(message.getMessage().hasChunk() && message.getMessage().getChunk().hasDedup()) {
    	  dedup = message.getMessage().getChunk().getDedup();
    	  LOG.info("@cesar: Pairing message with dedup: {}", dedup);
      }
      return message.getMessage().toBuilder()
          .setChunk(Chunk.newBuilder().setDedup(dedup).setData(UnsafeByteOperations.unsafeWrap(bytes)).build())
          .build();
    } finally {
      message.getBuffer().release();
    }
  }
}
