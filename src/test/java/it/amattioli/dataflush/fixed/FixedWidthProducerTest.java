package it.amattioli.dataflush.fixed;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

import org.mockito.Mockito;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;
import it.amattioli.dataflush.csv.CsvColumn;
import it.amattioli.dataflush.csv.CsvProducer;
import junit.framework.TestCase;

public class FixedWidthProducerTest extends TestCase {

	public void testCreateColumn() {
		FixedWidthProducer producer = new FixedWidthProducer();
		Column col = producer.createColumn();
		assertTrue(col instanceof FixedWidthColumn);
		assertTrue(producer.getColumns().contains(col));
	}
	
	public void testProduce() {
		FixedWidthProducer producer = new FixedWidthProducer();
		producer.addColumn(new FixedWidthColumn(0,9));
		producer.addColumn(new FixedWidthColumn(12,22));
		InputStream inputStream = getClass().getResourceAsStream("testProduce.data");
		producer.setReader(new InputStreamReader(inputStream));
		Consumer consumer = (Consumer)Mockito.mock(Consumer.class);
		producer.setConsumer(consumer);
		producer.produce();
		Record expected = new Record();
		expected.set(0, "FirstValue");
		expected.set(1, "SecondValue");
		((Consumer)Mockito.verify(consumer)).consume(expected);
	}

	public void testProduceTyped() throws Exception {
		FixedWidthProducer producer = new FixedWidthProducer();
		FixedWidthColumn col0 = new FixedWidthColumn(0,9);
		producer.addColumn(col0);
		FixedWidthColumn col1 = new FixedWidthColumn(11,14);
		col1.setIndex(new Integer(1));
		col1.setType("NUMBER");
		col1.setFormat("####");
		producer.addColumn(col1);
		FixedWidthColumn col2 = new FixedWidthColumn(16,25);
		col2.setIndex(new Integer(2));
		col2.setType("DATE");
		col2.setFormat("dd/MM/yyyy");
		producer.addColumn(col2);
		InputStream inputStream = getClass().getResourceAsStream("testProduceTyped.data");
		producer.setReader(new InputStreamReader(inputStream));
		Consumer consumer = (Consumer)Mockito.mock(Consumer.class);
		producer.setConsumer(consumer);
		producer.produce();
		Record expected = new Record();
		expected.set(0, "FirstValue");
		expected.set(1, new Long(1234));
		expected.set(2, (new SimpleDateFormat("dd/MM/yyyy")).parse("04/04/1970"));
		((Consumer)Mockito.verify(consumer)).consume(expected);
	}
}
