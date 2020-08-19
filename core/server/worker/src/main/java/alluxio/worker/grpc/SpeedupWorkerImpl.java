package alluxio.worker.grpc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import com.google.protobuf.ByteString;
import alluxio.grpc.GetChunkFromBlockRequest;
import alluxio.grpc.GetChunkFromBlockResponse;
import alluxio.grpc.SpeedupWorkerProtoGrpc;
import io.grpc.stub.StreamObserver;


public class SpeedupWorkerImpl extends SpeedupWorkerProtoGrpc.SpeedupWorkerProtoImplBase {

	private static final String BLOCK_STORE_MEM_PATH = "/mnt/ramdisk/alluxioworker";
	
	@Override
	public void getChunkFromBlock(GetChunkFromBlockRequest request, StreamObserver<GetChunkFromBlockResponse> responseObserver) {
		byte[] readData = getChunkFromBlockFile(request.getBlockId(), (int)request.getOffset(), (int)request.getSize());
		if(readData != null) {
			responseObserver.onNext(GetChunkFromBlockResponse.newBuilder().setData(ByteString.copyFrom(readData)).build());
		}
		else {
			// TODO: This can be done better
			responseObserver.onNext(GetChunkFromBlockResponse.newBuilder().setData(ByteString.EMPTY).build());
		}
		responseObserver.onCompleted();
	}
	
	// here i am supposed to have a block id. So i should know where is the block store path. Ill do this 
	// in a very simple fashion
	private byte[] getChunkFromBlockFile(String blockId, int offset, int len) {
		String thePath = BLOCK_STORE_MEM_PATH + File.separator + blockId;
		RandomAccessFile rand = null;
		try {
			rand = new RandomAccessFile(thePath, "r");
			rand.seek(offset);
			byte [] data = new byte[len];
			rand.read(data, 0, len);
			return data;
		}
		catch(Exception e){
			return null;
		}
		finally {
			if(rand != null) {
				try{
					rand.close();
				}
				catch(IOException e) {
					// nothing here
				}
			}
		}
	}
	
}
