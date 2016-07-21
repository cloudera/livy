package com.cloudera.livy.rsc.driver;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;


public class AddFileJob implements Job<Object> {

    private final String path;

    AddFileJob() {
        this(null);
    }

    public AddFileJob(String path) {
        this.path = path;
    }

    @Override
    public Object call(JobContext jc) throws Exception {
        jc.sc().addFile(path);
        return null;
    }

    public String getPath() {
        return path;
    }
}
