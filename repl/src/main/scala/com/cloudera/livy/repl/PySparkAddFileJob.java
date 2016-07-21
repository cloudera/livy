package com.cloudera.livy.repl;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class PySparkAddFileJob implements Job<Object> {

    private PySparkJobProcessor pySparkJobProcessor;
    private String path;

    public PySparkAddFileJob(String path, PySparkJobProcessor pySparkJobProcessor) {
        this.path = path;
        this.pySparkJobProcessor = pySparkJobProcessor;
    }

    @Override
    public Object call(JobContext jc) throws Exception {
        pySparkJobProcessor.addFile(path);
        return null;
    }
}