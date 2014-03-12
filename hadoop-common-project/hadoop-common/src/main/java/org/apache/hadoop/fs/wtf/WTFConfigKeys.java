package org.apache.hadoop.fs.wtf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
/** 
 * This class contains constants for configuration keys used
 * in the wtf file system. 
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class WTFConfigKeys extends CommonConfigurationKeys {
	  public static final String  WTF_BLOCK_SIZE_KEY = "wtf.blocksize";
	  public static final long    WTF_BLOCK_SIZE_DEFAULT = 64*1024;
	  public static final String  WTF_REPLICATION_KEY = "wtf.replication";
	  public static final long   WTF_REPLICATION_DEFAULT = 3;
	  public static final String  WTF_CLIENT_WRITE_PACKET_SIZE_KEY =
	                                                    "wtf.client-write-packet-size";
	  public static final int     WTF_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
	  public static final String  WTF_COORDINATOR_HOST_KEY = "wtf.coordinator.host";
	  public static final String  WTF_COORDINATOR_HOST_DEFAULT = "127.0.0.1";
	  public static final String  WTF_HYPERDEX_HOST_KEY = "wtf.hyperdex.host";
	  public static final String  WTF_HYPERDEX_HOST_DEFAULT = "127.0.0.1";
	  public static final String   WTF_COORDINATOR_PORT_KEY = "wtf.coordinator.port";
	  public static final short   WTF_COORDINATOR_PORT_DEFAULT = 1981;
	  public static final String  WTF_HYPERDEX_PORT_KEY = "wtf.hyperdex.port";
	  public static final short   WTF_HYPERDEX_PORT_DEFAULT = 1982;
}