package alluxio;

// @cesar: I added this class
public class SpeedupConstants {

	public enum FilePiece {
		ORCStripe,
		ORCColumn,
		ORCFooter,
		ERROR
	}
	
	public static FilePiece filePieceFromOrdinal(int ordinal) {
		switch(ordinal) {
			case 0: return FilePiece.ORCStripe;
			case 1: return FilePiece.ORCColumn;
			case 2: return FilePiece.ORCFooter;
			default: return FilePiece.ERROR;
		}
	}
	
	public int ordinalFromFilePiece(FilePiece piece) {
		switch(piece) {
			case ORCStripe: return 0;
			case ORCColumn: return 1;
			case ORCFooter: return 2;
			default: return -1000;
		}
	}
	
	
	
}
