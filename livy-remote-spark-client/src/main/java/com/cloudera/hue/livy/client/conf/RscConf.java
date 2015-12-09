package com.cloudera.hue.livy.client.conf;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RscConf extends Configuration {

    private static final Map<String, ConfVars> vars = new HashMap<String, ConfVars>();

    static {
        for (ConfVars confVar : ConfVars.values()) {
            vars.put(confVar.varname, confVar);
        }
    }

    public static enum ConfVars {
        SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT("hive.spark.client.server.connect.timeout",
                "90000ms"),
        SPARK_RPC_CLIENT_CONNECT_TIMEOUT("hive.spark.client.connect.timeout",
                "10000ms"),
        SPARK_RPC_CHANNEL_LOG_LEVEL("hive.spark.client.channel.log.level", null),
        SPARK_RPC_MAX_MESSAGE_SIZE("hive.spark.client.rpc.max.size", 50 * 1024 * 1024),
        SPARK_RPC_MAX_THREADS("hive.spark.client.rpc.threads", 8),
        SPARK_RPC_SECRET_RANDOM_BITS("hive.spark.client.secret.bits", "256");

        public final String varname;
        public final String defaultValStr;
        public final int defaultValInt;
        public final long defaultValLong;
        private final String defaultExpr;

        ConfVars(String varname, Object defaultVal){
            this.defaultExpr = defaultVal == null ? null : String.valueOf(defaultVal);

            if (defaultVal == null || defaultVal instanceof String) {
                this.varname = varname;
                this.defaultValStr = defaultVal == null ? null :defaultVal.toString();
                this.defaultValInt = -1;
                this.defaultValLong = -1;
            }
            else if (defaultVal instanceof Integer) {
                this.varname = varname;
                this.defaultValStr = null;
                this.defaultValInt = (Integer) defaultVal;
                this.defaultValLong = -1;
            }
            else if (defaultVal instanceof Long) {
                this.varname = varname;
                this.defaultValStr = null;
                this.defaultValInt = -1;
                this.defaultValLong = (Long) defaultVal;
            }
            else {
                throw new IllegalArgumentException("Not supported type value " + defaultVal.getClass() +
                        " for name " + varname);
            }
        }
    }

    public long getTimeVar(ConfVars var, TimeUnit outUnit) {
        long result;
        if(var.defaultValInt != -1) result = (long) var.defaultValInt;
        else if (var.defaultValStr != null){
            String long_time = var.defaultValStr;
            long_time = long_time.replace("ms", "");
            result = Long.parseLong(long_time);
        }
        else result = var.defaultValLong;
        return result;
    }

    public static ConfVars getConfVars(String name) {
        return vars.get(name);
    }
}
