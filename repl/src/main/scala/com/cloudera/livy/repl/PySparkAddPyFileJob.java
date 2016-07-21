package com.cloudera.livy.repl;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class PySparkAddPyFileJob implements Job<Object> {

    private PySparkJobProcessor pySparkJobProcessor;
    private String path;
    private SparkContext sparkContext;

    public PySparkAddPyFileJob(
           String path,
           PySparkJobProcessor pySparkJobProcessor,
           SparkContext sparkContext) {
        this.path = path;
        this.pySparkJobProcessor = pySparkJobProcessor;
        this.sparkContext = sparkContext;
    }

    @Override
    public Object call(JobContext jc) throws Exception {
        String localTmpDirPath = pySparkJobProcessor.getLocalTmpDirPath();
        File localCopyDir = new File(localTmpDirPath);
        synchronized (jc) {
            if (!localCopyDir.isDirectory() && !localCopyDir.mkdir()) {
                throw new IOException("Failed to create directory to add pyFile");
            }
        }
        URI uri = new URI(path);
        String name = uri.getFragment() != null ? uri.getFragment() : uri.getPath();
        name = new File(name).getName();
        File localCopy = new File(localCopyDir, name);

        if (localCopy.exists()) {
            throw new IOException(String.format("A file with name %s has " +
                    "already been uploaded.", name));
        }
        Configuration conf = sparkContext.hadoopConfiguration();
        FileSystem fs = FileSystem.get(uri, conf);
        fs.copyToLocalFile(new Path(uri), new Path(localCopy.toURI()));
        pySparkJobProcessor.addPyFile(path);
        return null;
    }
}
