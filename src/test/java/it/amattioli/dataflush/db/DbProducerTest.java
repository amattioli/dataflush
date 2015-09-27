package it.amattioli.dataflush.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.mockito.Mockito;

import junit.framework.TestCase;

public class DbProducerTest extends TestCase {
	
	public void testCreateColumn() {
		DbProducer producer = new DbProducer();
		Column col = producer.createColumn();
		assertTrue(col instanceof DbColumn);
		assertTrue(producer.getColumns().contains(col));
	}
	
	public void testSetQuery() {
		DbProducer producer = new DbProducer();
		String query = "select col1,col2 from my_table";
		producer.setQuery(query);
		String result = producer.getQuery();
		assertEquals(query, result);
	}

	public void testQueryCreation() {
		DbProducer producer = new DbProducer();
		producer.setTable("my_table");
		producer.addColumn(new DbColumn(0,"col1"));
		producer.addColumn(new DbColumn(1,"col2"));
		String query = producer.getQuery();
		assertEquals("select col1,col2 from my_table", query);
	}
	
	public void testQueryCreationWithWhere() {
		DbProducer producer = new DbProducer();
		producer.setTable("my_table");
		producer.addColumn(new DbColumn(0,"col1"));
		producer.addColumn(new DbColumn(1,"col2"));
		producer.setWhere("col1 is not null");
		String query = producer.getQuery();
		assertEquals("select col1,col2 from my_table where col1 is not null", query);
	}
	
	public void testProduceWiithQuerySet() throws Exception {
		String url = "jdbc:hsqldb:file:target/db/testProduce";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testProduce.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbProducer producer = new DbProducer();
		producer.setUrl(url);
		producer.setQuery("select col1,substr(col2,1,6) from my_table");
		producer.addColumn(new DbColumn(new Integer(0)));
		producer.addColumn(new DbColumn(new Integer(1)));
		Consumer consumer = (Consumer)Mockito.mock(Consumer.class);
		producer.setConsumer(consumer);
		producer.produce();
		
		Record expected = new Record();
		expected.set(0, "FirstValue");
		expected.set(1, "Second");
		((Consumer)Mockito.verify(consumer)).consume(expected);
		
		databaseTester.onTearDown();
	}
	
	public void testProduceWithQueryConstruction() throws Exception {
		String url = "jdbc:hsqldb:file:target/db/testProduce";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testProduce.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbProducer producer = new DbProducer();
		producer.setUrl(url);
		producer.setTable("my_table");
		producer.addColumn(new DbColumn(0,"col1"));
		producer.addColumn(new DbColumn(1,"col2"));
		Consumer consumer = (Consumer)Mockito.mock(Consumer.class);
		producer.setConsumer(consumer);
		producer.produce();
		
		Record expected = new Record();
		expected.set(0, "FirstValue");
		expected.set(1, "SecondValue");
		((Consumer)Mockito.verify(consumer)).consume(expected);
		
		databaseTester.onTearDown();
	}
	
	public void testProduceFormatted() throws Exception {
		Locale.setDefault(Locale.ENGLISH);
		String url = "jdbc:hsqldb:file:target/db/testProduce";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testProduceFormatted.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbProducer producer = new DbProducer();
		producer.setUrl(url);
		producer.setTable("my_table");
		DbColumn col3 = new DbColumn();
		col3.setName("col3");
		col3.setType("Number");
		col3.setFormat("##0.00");
		producer.addColumn(col3);
		DbColumn col4 = new DbColumn();
		col4.setName("col4");
		col4.setType("Date");
		col4.setFormat("dd/MM/yyyy");
		producer.addColumn(col4);
		DbColumn col5 = new DbColumn();
		col5.setName("col5");
		col5.setType("Number");
		col5.setFormat("##0");
		producer.addColumn(col5);
		Consumer consumer = (Consumer)Mockito.mock(Consumer.class);
		producer.setConsumer(consumer);
		producer.produce();
		
		Record expected = new Record();
		expected.set(0, "213.90");
		expected.set(1, "04/04/1970");
		expected.set(2, "6570");
		((Consumer)Mockito.verify(consumer)).consume(expected);
		
		databaseTester.onTearDown();
	}

	private void createTestTable(IDatabaseTester databaseTester) throws SQLException, Exception {
		Connection connection = databaseTester.getConnection().getConnection();
		ResultSet tables = connection.getMetaData().getTables(null, null, "MY_TABLE", null);
		if (tables.next()) {
			PreparedStatement stmt = connection.prepareStatement("drop table my_table");
			stmt.execute();
		}
		PreparedStatement stmt = connection.prepareStatement("create table my_table(col1 varchar(20), col2 varchar(20), col3 decimal(5,2), col4 date, col5 integer)");
		stmt.execute();
	}
	
}
