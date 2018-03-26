package com.ericsson.eea.ark.sli.grouping.memdb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.util.Set;
//import java.util.TreeSet;
import java.util.Map.Entry;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.log4j.Logger;

public class ImsiGrpMemDbPutSerialization implements Serialization<ImsiGrpMemDbPut> {

    protected static final Logger log = Logger.getLogger(ImsiGrpMemDbPutSerialization.class);

    @Override
    public boolean accept(Class<?> c) {
        return ImsiGrpMemDbPut.class.isAssignableFrom(c);
    }

    @Override
    public Serializer<ImsiGrpMemDbPut> getSerializer(Class<ImsiGrpMemDbPut> c) {
        return new ImsiGrpMemDbPutSerializer();
    }

    @Override
    public Deserializer<ImsiGrpMemDbPut> getDeserializer(Class<ImsiGrpMemDbPut> c) {
        return new ImsiGrpMemDbPutDeserializer();
    }

    private static class ImsiGrpMemDbPutDeserializer implements Deserializer<ImsiGrpMemDbPut> {
        private DataInputStream in;

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public ImsiGrpMemDbPut deserialize(ImsiGrpMemDbPut mutation) throws IOException {
            String line = in.readUTF();
            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(line);
                for (Entry<String, Object> e : json.entrySet()) {
                    String s = e.getKey();
                    Object o = e.getValue();
                    return new ImsiGrpMemDbPut(s,o.toString());
                }
            } catch (ParseException e) {
                throw new IOException(e);
            }
            return mutation;
        }

        @Override
        public void open(InputStream in) throws IOException {
            this.in = new DataInputStream(in);
        }

    }

    private static class ImsiGrpMemDbPutSerializer implements Serializer<ImsiGrpMemDbPut> {
        private DataOutputStream out;

        @Override
        public void close() throws IOException {
            out.close();
        }

        @Override
        public void open(OutputStream out) throws IOException {
            this.out = new DataOutputStream(out);
        }

        @Override
        public void serialize(ImsiGrpMemDbPut mutation) throws IOException {
            log.debug("dob length on entry: " + out.size());
            String jsonString = mutation.toJsonString();
            log.debug("about to serialize a memdb put mutation object to: \"" + jsonString + "\"");
            out.writeUTF(jsonString);
            log.debug("out length on exit: " + out.size());
        }
    }
}
