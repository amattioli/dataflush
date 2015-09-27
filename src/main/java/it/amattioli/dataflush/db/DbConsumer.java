package it.amattioli.dataflush.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

public class DbConsumer extends Consumer {
	private Connection connection;
	private String url;
	private String driver;
	private String table;
	private String sql;
	private Locale locale = Locale.getDefault();
	private ArrayList columns = new ArrayList();
	private PreparedStatement stmt;
	
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
	
	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
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
		if (connection == null) {
			if (getDriver() != null) {
				try {
					Class.forName(getDriver());
				} catch(ClassNotFoundException e) {
					throw new IllegalStateException("Unknown driver: "+getDriver());
				}
			}
			connection = DriverManager.getConnection(getUrl());
		}
		return connection;
	}
	
	private PreparedStatement getStatement() throws SQLException {
		if (stmt == null) {
			stmt = getConnection().prepareStatement(getInsert());
		}
		return stmt;
	}
	
	public void consume(Record record) {
		if (record != Record.NULL) {
			try {
				int idx = 0;
				for (Iterator iter = columns.iterator(); iter.hasNext();) {
					DbColumn column = (DbColumn)iter.next();
					column.writeTo(getStatement(), 
							       column.getIndex().intValue() + 1, 
							       record.get(idx));
					idx++;
				}
				getStatement().execute();
			} catch(SQLException e) {
				throw new RuntimeException("Error writing to database. "+e.getMessage());
			}
		} else {
			try {
				getConnection().commit();
				getConnection().close();
			} catch (SQLException e) {
				throw new RuntimeException("Error committing database transaction. "+e.getMessage());
			}
		}
	}

	public String getInsert() {
		if (sql != null) {
			return sql;
		} else {
			return createInsert();
		}
	}

	private String createInsert() {
		StringBuffer headBuffer = new StringBuffer("insert into ");
		StringBuffer valuesBuffer = new StringBuffer(" values (");
		headBuffer.append(getTable());
		headBuffer.append('(');
		for (Iterator iter = columns.iterator(); iter.hasNext();) {
			Column column = (Column)iter.next();
			headBuffer.append(column.getName());
			valuesBuffer.append('?');
			if (iter.hasNext()) {
				headBuffer.append(',');
				valuesBuffer.append(",");
			}
		}
		headBuffer.append(')');
		valuesBuffer.append(')');
		return headBuffer.toString() + valuesBuffer.toString();
	}

}
