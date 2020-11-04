package com.gslab.pepper.loadgen.impl;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.loadgen.BaseLoadGenerator;
/**
 * The PlaintTextLoadGenerator is custom load generator class gets invoked from iteratorStart of PlainTextConfigElement class
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */

public class PlaintTextLoadGenerator implements BaseLoadGenerator {

    private transient Iterator<String> messageIterator = null;

    private transient SchemaProcessor schemaProcessor = new SchemaProcessor();

    private static final Logger log = LogManager.getLogger(PlaintTextLoadGenerator.class);

    /**
     * PlaintTextLoadGenerator constructor which initializes message iterator using schemaProcessor
     * @param jsonSchema
     */
    public PlaintTextLoadGenerator(String jsonSchema) throws PepperBoxException {

        try {
            this.messageIterator = schemaProcessor.getPlainTextMessageIterator(jsonSchema);
        }catch (Exception exc){
            log.error("Please make sure that expressions functions are already defined and parameters are correctly passed.", exc);
            throw new PepperBoxException(exc);
        }
    }

    /**
     * sends next message to PlainTextConfigElement
     * @return
     */
    @Override
    public Object nextMessage() {
        return messageIterator.next();
    }
}
