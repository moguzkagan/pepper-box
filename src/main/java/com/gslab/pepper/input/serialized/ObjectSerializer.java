package com.gslab.pepper.input.serialized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The ObjectSerializer is custom Object serializer for kafka producer. This class takes object as input and returns byte array.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class ObjectSerializer implements Serializer {

    private static Logger log = LogManager.getLogger(ObjectSerializer.class.getName());
    @Override
    public void configure(Map map, boolean b) {
        //TODO
    }

    @Override
    public byte[] serialize(String s, Object o) {

        byte[] retVal = null;

        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
        ) {

            out.writeObject(o);
            out.flush();
            retVal = bos.toByteArray();
        } catch (IOException e) {
     	   log.error( "Failed to serialize object", e);
        }
        return retVal;
    }

    @Override
    public void close() {
        //TODO
    }
}
