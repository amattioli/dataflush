package it.amattioli.dataflush.fixed;

import java.io.StringWriter;
import java.text.SimpleDateFormat;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Record;
import it.amattioli.dataflush.csv.CsvColumn;
import it.amattioli.dataflush.csv.CsvConsumer;
import it.amattioli.dataflush.testutils.FileAssert;
import junit.framework.TestCase;

public class FixedWidthConsumerTest extends TestCase {

	public void testCreateColumn() {
		FixedWidthConsumer consumer = new FixedWidthConsumer();
		Column col = consumer.createColumn();
		assertTrue(col instanceof FixedWidthColumn);
		assertTrue(consumer.getColumns().contains(col));
	}
	
	public void testConsume() throws Exception {
		FixedWidthConsumer consumer = new FixedWidthConsumer();
		StringWriter writer = new StringWriter();
		consumer.setWriter(writer);
		consumer.addColumn(new FixedWidthColumn(0,9));
		consumer.addColumn(new FixedWidthColumn(12,22));
		Record record = new Record();
		record.set(0, "FirstValue");
		record.set(1, "SecondValue");
		consumer.consume(record);
		consumer.consume(Record.NULL);
		FileAssert.assertSameContent("/it/amattioli/dataflush/fixed/testConsume.data",writer);
	}
	
	public void testConsumeTyped() throws Exception {
		FixedWidthConsumer consumer = new FixedWidthConsumer();
		StringWriter writer = new StringWriter();
		consumer.setWriter(writer);
		FixedWidthColumn col0 = new FixedWidthColumn(0,10);
		consumer.addColumn(col0);
		FixedWidthColumn col1 = new FixedWidthColumn(11,15);
		col1.setType("NUMBER");
		col1.setFormat("####");
		consumer.addColumn(col1);
		FixedWidthColumn col2 = new FixedWidthColumn(16,25);
		col2.setType("DATE");
		col2.setFormat("dd/MM/yyyy");
		consumer.addColumn(col2);
		Record record = new Record();
		record.set(0, "FirstValue");
		record.set(1, new Long(1234));
		record.set(2, (new SimpleDateFormat("dd/MM/yyyy")).parse("04/04/1970"));
		consumer.consume(record);
		consumer.consume(Record.NULL);
		FileAssert.assertSameContent("/it/amattioli/dataflush/fixed/testConsumeTyped.data",writer);
	}

}
