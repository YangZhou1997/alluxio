package alluxio.underfs.speedup;

import alluxio.conf.PropertyKey;

public class SpeedupUnderFileSystemPropertyKey {

	public static final PropertyKey SPEEDUP_HOST_UFS_PROPERTY =
	      new PropertyKey.Builder(Name.SPEEDUP_HOST_UFS_PROPERTY)
	          .setDescription("This is the host that we will be connecting to. It has to be specified when mounting")
	          .setDefaultValue("")
	          .build();
	
	public static final PropertyKey SPEEDUP_PORT_UFS_PROPERTY =
		      new PropertyKey.Builder(Name.SPEEDUP_PORT_UFS_PROPERTY)
		          .setDescription("This is the port on the host that we will be connecting to. Defaults to 19998")
		          .setDefaultValue(19998)
		          .build();

	public static final PropertyKey SPEEDUP_BASE_UFS_PROPERTY =
		      new PropertyKey.Builder(Name.SPEEDUP_BASE_UFS_PROPERTY)
		          .setDescription("This is the base where this instance will be mounted. e.g. mount x/y speedup://blabla, then base is x/y")
		          .setDefaultValue("")
		          .build();
	

	public static final class Name {
		public static final String SPEEDUP_HOST_UFS_PROPERTY = "speedup.ufs.host";
		public static final String SPEEDUP_PORT_UFS_PROPERTY = "speedup.ufs.port";
		public static final String SPEEDUP_BASE_UFS_PROPERTY = "speedup.ufs.base";
	}
	
}
