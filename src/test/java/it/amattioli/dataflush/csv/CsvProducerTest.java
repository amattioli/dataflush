package it.amattioli.dataflush.csv;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import junit.framework.TestCase;

import org.mockito.Mockito;

public class CsvProducerTest extends TestCase {
	
	public void testCreateColumn() {
		CsvProducer producer = new CsvProducer();
		Column col = producer.createColumn();
		assertTrue(col instanceof CsvColumn);
		assertTrue(producer.getColumns().contains(col));
	}
	
	public void testProduce() {
		CsvProducer producer = new CsvProducer();
		producer.addColumn(new CsvColumn(new Integer(0)));
		producer.addColumn(new CsvColumn(new Integer(1)));
		InputStream inputStream = getClass().getResourceAsStream("testProduce.csv");
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
		CsvProducer producer = new CsvProducer();
		CsvColumn col0 = new CsvColumn();
		col0.setIndex(new Integer(0));
		producer.addColumn(col0);
		CsvColumn col1 = new CsvColumn();
		col1.setIndex(new Integer(1));
		col1.setType("NUMBER");
		col1.setFormat("####");
		producer.addColumn(col1);
		CsvColumn col2 = new CsvColumn();
		col2.setIndex(new Integer(2));
		col2.setType("DATE");
		col2.setFormat("dd/MM/yyyy");
		producer.addColumn(col2);
		InputStream inputStream = getClass().getResourceAsStream("testProduceTyped.csv");
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
	
	public void testProduceWithHeaders() {
		CsvProducer producer = new CsvProducer();
		producer.setHeaders(true);
		producer.addColumn(new CsvColumn("FIRSTCOL"));
		producer.addColumn(new CsvColumn("SECONDCOL"));
		InputStream inputStream = getClass().getResourceAsStream("testProduceWithHeaders.csv");
		producer.setReader(new InputStreamReader(inputStream));
		Consumer consumer = (Consumer)Mockito.mock(Consumer.class);
		producer.setConsumer(consumer);
		producer.produce();
		Record expected = new Record();
		expected.set(0, "FirstValue");
		expected.set(1, "SecondValue");
		((Consumer)Mockito.verify(consumer)).consume(expected);
	}

}
