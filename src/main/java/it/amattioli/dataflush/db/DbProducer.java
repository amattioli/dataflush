package it.amattioli.dataflush.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Producer;
import it.amattioli.dataflush.Record;

public class DbProducer extends Producer {
	private Consumer consumer;
	private String url;
	private String driver;
	private String table;
	private String where;
	private String query;
	private Locale locale = Locale.getDefault();
	private ArrayList columns = new ArrayList();
	
	public Consumer getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public void addColumn(Column column) {
		column.setLocale(getLocale());
		columns.add(column);
	}

	public Column createColumn() {
		Column newColumn = new DbColumn();
		addColumn(newColumn);
		return newColumn;
	}
	
	public List getColumns() {
		return columns;
	}

	private Connection getConnection() throws SQLException {
		if (getDriver() != null) {
			try {
				Class.forName(getDriver());
			} catch(ClassNotFoundException e) {
				throw new IllegalStateException("Unknown driver: "+getDriver());
			}
		}
		return DriverManager.getConnection(getUrl());
	}

	public String getQuery() {
		if (query != null) {
			return query;
		}
		StringBuffer buffer = new StringBuffer("select ");
		for (Iterator iter = columns.iterator(); iter.hasNext();) {
			Column column = (Column)iter.next();
			buffer.append(column.getName());
			buffer.append(',');
		}
		buffer.deleteCharAt(buffer.length()-1);
		buffer.append(" from ");
		buffer.append(table);
		if (where != null) {
			buffer.append(" where ");
			buffer.append(where);
		}
		return buffer.toString();
	}
	
	public void setQuery(String query) {
		this.query = query;
	}

	private Record createRecord(ResultSet result) throws SQLException {
		Record record = new Record();
		int index = 0;
		for (Iterator iter = columns.iterator(); iter.hasNext();) {
			DbColumn column = (DbColumn)iter.next();
			record.set(index++, column.readFrom(result));
		}
		return record;
	}
	
	public void produce() {
		Connection conn = null;
		try {
			conn = getConnection();
			String query = getQuery();
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet result = statement.executeQuery();
			while (result.next()) {
				Record record = createRecord(result);
				consumer.consume(record);
			}
			consumer.consume(Record.NULL);
		} catch (SQLException e) {
			throw new RuntimeException("Error reading database. "+e.getMessage());
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				throw new RuntimeException("Error closing database connection. "+e.getMessage());
			}
		}
	}

}
