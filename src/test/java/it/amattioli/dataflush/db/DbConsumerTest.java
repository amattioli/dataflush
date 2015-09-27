package it.amattioli.dataflush.db;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import org.dbunit.Assertion;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;

import junit.framework.TestCase;

public class DbConsumerTest extends TestCase {
	
	public void testCreateColumn() {
		DbConsumer consumer = new DbConsumer();
		Column col = consumer.createColumn();
		assertTrue(col instanceof DbColumn);
		assertTrue(consumer.getColumns().contains(col));
	}

	public void testCreateInsert() {
		String expectedInsert = "insert into my_table(col1,col2) values (?,?)";
		DbConsumer consumer = new DbConsumer();
		consumer.setTable("my_table");
		consumer.addColumn(new DbColumn(0,"col1"));
		consumer.addColumn(new DbColumn(1,"col2"));
		String insert = consumer.getInsert();
		assertEquals(expectedInsert, insert);
	}
	
	public void testInsertProvided() {
		String expectedInsert = "insert into my_table(col1,col2) values (?,?)";
		DbConsumer consumer = new DbConsumer();
		consumer.setSql(expectedInsert);
		String insert = consumer.getInsert();
		assertEquals(expectedInsert, insert);
	}
	
	public void testConsumeInsertCreated() throws Exception {
		String url = "jdbc:hsqldb:file:target/db/testConsume";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testConsume.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbConsumer consumer = new DbConsumer();
		consumer.setUrl(url);
		consumer.setTable("my_table");
		consumer.addColumn(new DbColumn(0,"col1"));
		consumer.addColumn(new DbColumn(1,"col2"));
		
		Record toBeConsumed = new Record();
		toBeConsumed.set(0, "FirstValue");
		toBeConsumed.set(1, "SecondValue");
		consumer.consume(toBeConsumed);
		
		assertTableContent(databaseTester, "expectedTestConsume.xml", "my_table");
		
		databaseTester.onTearDown();
	}
	
	public void testConsumeInsertProvided() throws Exception {
		String url = "jdbc:hsqldb:file:target/db/testConsume";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testConsume.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbConsumer consumer = new DbConsumer();
		consumer.setUrl(url);
		consumer.setSql("insert into my_table(col2,col1) values (?,?)");
		consumer.addColumn(new DbColumn(1));
		consumer.addColumn(new DbColumn(0));
		
		Record toBeConsumed = new Record();
		toBeConsumed.set(0, "FirstValue");
		toBeConsumed.set(1, "SecondValue");
		consumer.consume(toBeConsumed);
		
		assertTableContent(databaseTester, "expectedTestConsume.xml", "my_table");
		
		databaseTester.onTearDown();
	}
	
	public void testConsumeFormatted() throws Exception {
		Locale.setDefault(Locale.ENGLISH);
		String url = "jdbc:hsqldb:file:target/db/testConsume";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testConsume.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbConsumer consumer = new DbConsumer();
		consumer.setUrl(url);
		consumer.setTable("my_table");
		DbColumn col3 = new DbColumn(0);
		col3.setName("col3");
		col3.setType("Number");
		col3.setFormat("##0.00");
		consumer.addColumn(col3);
		DbColumn col4 = new DbColumn(1);
		col4.setName("col4");
		col4.setType("Date");
		col4.setFormat("dd/MM/yyyy");
		consumer.addColumn(col4);
		
		Record toBeConsumed = new Record();
		toBeConsumed.set(0, "123.45");
		toBeConsumed.set(1, "04/04/1970");
		consumer.consume(toBeConsumed);
		
		assertTableContent(databaseTester, "expectedTestConsumeFormatted.xml", "my_table");
		
		databaseTester.onTearDown();
	}
	
	public void testConsumeUnparseable() throws Exception {
		String url = "jdbc:hsqldb:file:target/db/testConsume";
		IDatabaseTester databaseTester = new JdbcDatabaseTester("org.hsqldb.jdbcDriver", url, "sa", "");
		createTestTable(databaseTester);
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("testConsume.xml"));
		databaseTester.setDataSet( dataSet );
		databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
		databaseTester.onSetup();
		
		DbConsumer consumer = new DbConsumer();
		consumer.setUrl(url);
		consumer.setTable("my_table");
		DbColumn col3 = new DbColumn(0);
		col3.setName("col3");
		col3.setType("Number");
		col3.setFormat("##0.00");
		consumer.addColumn(col3);
		
		Record toBeConsumed = new Record();
		toBeConsumed.set(0, "Hello");
		try {
			consumer.consume(toBeConsumed);
			fail("Should throw exception");
		} catch(Exception e) {
			assertEquals("Cannot parse Hello as a Number", e.getMessage());
		} finally {
			databaseTester.onTearDown();
		}
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
	
	public void assertTableContent(IDatabaseTester databaseTester, String expectedDataSetResource, String tableName) throws Exception {
		QueryDataSet databaseDataSet = new QueryDataSet(databaseTester.getConnection());
		databaseDataSet.addTable(tableName);
        ITable actualTable = databaseDataSet.getTable(tableName);
        
        IDataSet baseExpectedDataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream(expectedDataSetResource));
        ReplacementDataSet expectedDataSet = new ReplacementDataSet(baseExpectedDataSet);
        //expectedDataSet.addReplacementObject("TODAY", Day.today().getInitTime());
        expectedDataSet.addReplacementObject("NULL", null);
        ITable expectedTable = expectedDataSet.getTable(tableName);
        
        Assertion.assertEquals(expectedTable, actualTable);
	}
	
}
