
library(SparkR)
port <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "")
SparkR:::connectBackend("localhost", port)

assign(".scStartTime", as.integer(Sys.time()), envir = SparkR:::.sparkREnv)
# get SparkConf/SparkContext/SQLContext from JVM side rather than creating them
# from R. So that we can share the same SparkContext/SQLContext in one jvm.
assign(".sc", SparkR:::callJStatic("com.cloudera.livy.repl.SparkRInterpreter", "getSparkContext"), envir = SparkR:::.sparkREnv)
assign("sc", get(".sc", envir = SparkR:::.sparkREnv), envir=.GlobalEnv)
assign(".sqlc", SparkR:::callJStatic("com.cloudera.livy.repl.SparkRInterpreter", "getSQLContext"), envir = SparkR:::.sparkREnv)
assign("sqlContext", get(".sqlc", envir = SparkR:::.sparkREnv), envir = .GlobalEnv)
