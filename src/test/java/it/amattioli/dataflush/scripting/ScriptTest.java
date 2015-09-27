package it.amattioli.dataflush.scripting;

import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Producer;
import it.amattioli.dataflush.csv.CsvColumn;
import it.amattioli.dataflush.csv.CsvConsumer;
import it.amattioli.dataflush.csv.CsvProducer;
import it.amattioli.dataflush.db.DbConsumer;
import it.amattioli.dataflush.db.DbProducer;
import it.amattioli.dataflush.fixed.FixedWidthColumn;
import junit.framework.TestCase;

public class ScriptTest extends TestCase {
	
	public void testCreateCsvProducer() {
		Script script = new Script();
		Producer producer = script.createProducer("FROM CSV");
		assertTrue(producer instanceof CsvProducer);
	}
	
	public void testCreateDbProducer() {
		Script script = new Script();
		Producer producer = script.createProducer("FROM DATABASE");
		assertTrue(producer instanceof DbProducer);
	}
	
	public void testUnknownProducer() {
		Script script = new Script();
		try {
			script.createProducer("FROM SOMETHING");
			fail("Should throw exception");
		} catch(Exception e) {
			assertEquals("Unknown source", e.getMessage());
		}
	}
	
	public void testCreateCsvConsumer() {
		Script script = new Script();
		Consumer consumer = script.createConsumer("TO CSV");
		assertTrue(consumer instanceof CsvConsumer);
	}
	
	public void testCreateDbConsumer() {
		Script script = new Script();
		Consumer consumer = script.createConsumer("TO DATABASE");
		assertTrue(consumer instanceof DbConsumer);
	}
	
	public void testUnknownConsumer() {
		Script script = new Script();
		try {
			script.createConsumer("TO SOMETHING");
			fail("Should throw exception");
		} catch(Exception e) {
			assertEquals("Unknown destination", e.getMessage());
		}
	}

	public void testStringAttribute() {
		Script script = new Script();
		DbConsumer bean = new DbConsumer();
		String url = "jdbc:hsqldb:file:target/db/test";
		script.setAttribute(bean, "url: "+url);
		assertEquals(url, bean.getUrl());
	}
	
	public void testBooleanAttribute() {
		Script script = new Script();
		CsvConsumer bean = new CsvConsumer();
		script.setAttribute(bean, "headers: true");
		assertTrue(bean.isHeaders());
	}
	
	public void testAttributeBeforeSection() {
		Script script = new Script();
		try {
			script.processLine("headers: true");
			fail("Should throw exception");
		} catch(Exception e) {
			assertEquals("No FROM, TO or COLUMN section started", e.getMessage());
		}
	}

	public void testProducerColumn() {
		Script script = new Script();
		script.addProducer("FROM CSV");
		script.addConsumer("TO CSV");
		script.addProducerCol("colName");
		assertTrue(script.getProducer().getColumns().contains(new CsvColumn("colName")));
	}
	
	public void testConsumerColumn() {
		Script script = new Script();
		script.addProducer("FROM CSV");
		script.addConsumer("TO CSV");
		script.addConsumerCol("colName");
		assertTrue(script.getConsumer().getColumns().contains(new CsvColumn("colName")));
	}
	
	public void testColumn() {
		Script script = new Script();
		script.addProducer("FROM CSV");
		script.addConsumer("TO CSV");
		script.addColumn("COLUMN producerCol => consumerCol");
		assertTrue(script.getProducer().getColumns().contains(new CsvColumn("producerCol")));
		assertTrue(script.getConsumer().getColumns().contains(new CsvColumn("consumerCol")));
	}
	
	public void testColumnWithSpaces() {
		Script script = new Script();
		script.addProducer("FROM CSV");
		script.addConsumer("TO CSV");
		script.addColumn("COLUMN producerCol   =>   consumerCol");
		assertTrue(script.getProducer().getColumns().contains(new CsvColumn("producerCol")));
		assertTrue(script.getConsumer().getColumns().contains(new CsvColumn("consumerCol")));
	}
	
	public void testColumnWithTabs() {
		Script script = new Script();
		script.addProducer("FROM CSV");
		script.addConsumer("TO CSV");
		script.addColumn("COLUMN producerCol\t\t=>\t consumerCol");
		assertTrue(script.getProducer().getColumns().contains(new CsvColumn("producerCol")));
		assertTrue(script.getConsumer().getColumns().contains(new CsvColumn("consumerCol")));
	}
	
	public void testWrongColumnDefinition() {
		Script script = new Script();
		script.addProducer("FROM CSV");
		script.addConsumer("TO CSV");
		try {
			script.addColumn("COLUMN producerCol ");
			fail("Should throw exception");
		} catch(Exception e) {
			assertEquals("Wrong column definition", e.getMessage());
		}
	}
	
	public void testDelimitedColumn() {
		Script script = new Script();
		script.addProducer("FROM FIXED WIDTH");
		script.addConsumer("TO FIXED WIDTH");
		script.addColumn("COLUMN [10,20] => [15,25]");
		assertTrue(script.getProducer().getColumns().contains(new FixedWidthColumn(10,20)));
		assertTrue(script.getConsumer().getColumns().contains(new FixedWidthColumn(15,25)));
	}
}
