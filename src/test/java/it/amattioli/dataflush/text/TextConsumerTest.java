package it.amattioli.dataflush.text;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Record;
import it.amattioli.dataflush.testutils.FileAssert;
import junit.framework.TestCase;

import java.io.StringWriter;

/**
 * Created by andrea on 23/12/2015.
 */
public class TextConsumerTest extends TestCase {

    public void testCreateColumn() {
        TextConsumer consumer = new TextConsumer();
        Column col = consumer.createColumn();
        assertTrue(col instanceof TextColumn);
        assertTrue(consumer.getColumns().contains(col));
    }

    public void testConsume() throws Exception {
        TextConsumer consumer = new TextConsumer();
        StringWriter writer = new StringWriter();
        consumer.setWriter(writer);
        consumer.addColumn(new TextColumn(new Integer(1)));
        consumer.addColumn(new TextColumn(new Integer(0)));
        Record record = new Record();
        record.set(0, "FirstValue");
        record.set(1, "SecondValue");
        consumer.consume(record);
        consumer.consume(Record.NULL);
        FileAssert.assertSameContent("/it/amattioli/dataflush/text/testConsume.txt", writer);
    }
}
