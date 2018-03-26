package com.ericsson.eea.ark.sli.grouping.memdb;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ericsson.eea.ark.sli.groups.client.util.GrpMapCliUtil;

public class ImsiGrpMemDbOutputFormat extends OutputFormat<Text, ImsiGrpMemDbPut> implements Configurable {

    /** The configuration. */
    private Configuration conf = null;

    private GrpMapCliUtil gmcu;

    /**
     * Writes the reducer output to MemDb
     *
     * {@code Text}  The type of the key.
     */
    protected static class ImsiGroupMemDbRecordWriter
    extends RecordWriter<Text, ImsiGrpMemDbPut> {

        private GrpMapCliUtil gmcu;

        ImsiGroupMemDbRecordWriter(GrpMapCliUtil gmcu) {
            this.gmcu = gmcu;
        }

        /**
         * Closes the writer, in this case flush table commits.
         *
         * @param context  The context.
         * @throws IOException When closing the writer fails.
         * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
         */
        @Override
        public void close(TaskAttemptContext context)
                throws IOException {
            gmcu.close();
        }

        /**
         * Writes a key/value pair into the table.
         *
         * @param keyText  The key.
         * @param value  The value.
         * @throws IOException When writing fails.
         * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
         */
        @Override
        public void write(Text keyText, ImsiGrpMemDbPut value)
                throws IOException {
               gmcu.merge(value.toUserGroups(keyText.toString()));
        }
    }

    /**
     * Creates a new record writer.
     *
     * @param context  The current task context.
     * @return The newly created writer instance.
     * @throws IOException When creating the writer fails.
     * @throws InterruptedException When the jobs is cancelled.
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordWriter<Text, ImsiGrpMemDbPut> getRecordWriter(
            TaskAttemptContext context)
                    throws IOException, InterruptedException {
        return new ImsiGroupMemDbRecordWriter(gmcu);
    }

    /**
     * Checks if the output target exists.
     *
     * @param context  The current context.
     * @throws IOException When the check fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
    InterruptedException {
        // Check if the table exists?

    }

    /**
     * Returns the output committer.
     *
     * @param context  The current context.
     * @return The committer.
     * @throws IOException When creating the committer fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new ImsiGrpMemDbOutputCommitter();
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration otherConf) {
        this.conf = new Configuration(otherConf);
        if ((this.gmcu = GrpMapCliUtil.instance(this.conf)) == null) {
            throw new IllegalArgumentException("GrpMapCliUtil instance is null!");
        }
    }
}
