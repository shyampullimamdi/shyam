package com.ericsson.eea.ark.sli.grouping.services.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.sli.grouping.data.BaseTest;

public class Test_GroupMapperDriver extends BaseTest {

    protected static Logger logger = null;

    public static void main(String[] args) throws Throwable {
        Test_GroupMapperDriver me = new Test_GroupMapperDriver();
        Test_GroupMapperDriver.setUpBeforeClass();
        me.setupBefore();
        Throwable t = null;
        try {
            me.test_HadoopDriver_run();
        } catch (Throwable tx) {
            t = tx;
        }
        me.tearDownAfter();
        Test_GroupMapperDriver.tearDownAfterClass();
        if (t != null) throw t;
    }

    @Ignore
    public void test_HadoopDriver_run() throws Exception {

//	      ParquetFileWriter w,
//	      WriteSupport<T> writeSupport,
//	      MessageType schema,
//	      Map<String, String> extraMetaData,
//	      int blockSize, int pageSize,
//	      BytesCompressor compressor,
//	      int dictionaryPageSize,
//	      boolean enableDictionary,
//	      boolean validating,
//	      WriterVersion writerVersion
//
//		Schema schema = new Schema.Parser().parse(new File("C://Users//emademb//cea//cea_product//offline//pre-processor//csv//config//csv//csv.avsc"));
//		List<Type> fieldsList = new ArrayList<>();
//		MessageType messageType = new MessageType("CsvRecord", fieldsList);
//		fieldsList.add(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "fields"));
//
//		ParquetRecordWriter w = new ParquetRecordWriter(
//				new ParquetFileWriter(
//						new Configuration(),
//						messageType,
//						new Path("C://Users//emademb//cea//cea_product//offline//pre-processor//csv//config//csv//csvout.parquet")),
//				new AvroWriteSupport(messageType, schema),
//				messageType,
//				new HashMap<String,String>(),
//				1024,
//				1024,
//				null, //new BytesCompressor(),
//				256, false, false, null);
//
//		System.exit(0);
        Test_GroupMapperDriver.conf.set("fs.defaultFS", origDefaultFS);

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(new Path("output"), true); // delete old output

        int rc;

        try {

            @SuppressWarnings("rawtypes")
            GroupMapperDriver driver = new GroupMapperDriver(conf);

            rc = driver.run(new String[] {});

        } catch (Throwable t) {
            String msg = Config._cn_
                    + ": Cannot create/execute GroupMapperDriver.";
            logger().error(msg, t);
            throw t;
        }

        Assert.assertEquals(rc, 0);

    }

    @Override
    protected Logger logger() {
        if (logger == null) { synchronized(Test_GroupMapperDriver.class) { if (logger == null) {
            logger = Logger.getLogger(Test_GroupMapperDriver.class);
        }}}
        return logger;
    }
}
