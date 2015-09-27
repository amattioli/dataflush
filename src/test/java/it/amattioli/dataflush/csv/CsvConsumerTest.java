package it.amattioli.dataflush.csv;

import java.io.StringWriter;
import java.text.SimpleDateFormat;

import junit.framework.TestCase;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Record;
import it.amattioli.dataflush.testutils.FileAssert;

public class CsvConsumerTest extends TestCase {
	
	public void testCreateColumn() {
		CsvConsumer consumer = new CsvConsumer();
		Column col = consumer.createColumn();
		assertTrue(col instanceof CsvColumn);
		assertTrue(consumer.getColumns().contains(col));
	}

	public void testConsume() throws Exception {
		CsvConsumer consumer = new CsvConsumer();
		StringWriter writer = new StringWriter();
		consumer.setWriter(writer);
		consumer.addColumn(new CsvColumn(new Integer(1)));
		consumer.addColumn(new CsvColumn(new Integer(0)));
		Record record = new Record();
		record.set(0, "FirstValue");
		record.set(1, "SecondValue");
		consumer.consume(record);
		consumer.consume(Record.NULL);
		FileAssert.assertSameContent("/it/amattioli/dataflush/csv/testConsume.csv",writer);
	}
	
	public void testConsumeTyped() throws Exception {
		CsvConsumer consumer = new CsvConsumer();
		StringWriter writer = new StringWriter();
		consumer.setWriter(writer);
		CsvColumn col0 = new CsvColumn();
		col0.setIndex(new Integer(0));
		consumer.addColumn(col0);
		CsvColumn col1 = new CsvColumn();
		col1.setIndex(new Integer(1));
		col1.setType("NUMBER");
		col1.setFormat("####");
		consumer.addColumn(col1);
		CsvColumn col2 = new CsvColumn();
		col2.setIndex(new Integer(2));
		col2.setType("DATE");
		col2.setFormat("dd/MM/yyyy");
		consumer.addColumn(col2);
		Record record = new Record();
		record.set(0, "FirstValue");
		record.set(1, new Long(1234));
		record.set(2, (new SimpleDateFormat("dd/MM/yyyy")).parse("04/04/1970"));
		consumer.consume(record);
		consumer.consume(Record.NULL);
		FileAssert.assertSameContent("/it/amattioli/dataflush/csv/testConsumeTyped.csv",writer);
	}
	
	public void testConsumeWithHeaders() throws Exception {
		CsvConsumer consumer = new CsvConsumer();
		consumer.setHeaders(true);
		StringWriter writer = new StringWriter();
		consumer.setWriter(writer);
		consumer.addColumn(new CsvColumn("HEAD1",new Integer(1)));
		consumer.addColumn(new CsvColumn("HEAD2",new Integer(0)));
		Record record = new Record();
		record.set(0, "FirstValue");
		record.set(1, "SecondValue");
		consumer.consume(record);
		consumer.consume(Record.NULL);
		FileAssert.assertSameContent("/it/amattioli/dataflush/csv/testConsumeWithHeaders.csv",writer);
	}

}
