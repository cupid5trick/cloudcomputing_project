package redis;

public class RedisConf {
    private final static String clusterIp = "192.168.43.128";
    private final static int clusterPort = 6379;

    public static String getClusterIp() { return clusterIp; }
    public static int getClusterPort() { return clusterPort; }
}
