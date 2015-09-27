package it.amattioli.dataflush.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Format;
import java.text.ParseException;

import it.amattioli.dataflush.Column;

public class DbColumn extends Column {

	public DbColumn() {
		
	}
	
	public DbColumn(Integer index) {
		setIndex(index);
	}
	
	public DbColumn(int index) {
		this(new Integer(index));
	}
	
	public DbColumn(Integer index, String name) {
		this(index);
		setName(name);
	}
	
	public DbColumn(int index, String name) {
		this(new Integer(index), name);
	}
	
	public boolean equals(Object obj) {
		if (!(obj instanceof DbColumn)) {
			return false;
		}
		if (getName() == null) {
			return ((DbColumn)obj).getName() == null;
		}
		return getName().equals(((DbColumn)obj).getName());
	}
	
	public Object readFrom(ResultSet result) throws SQLException {
		Object obj;
		if (getName() != null) {
			obj = result.getObject(getName());
		} else {
			obj = result.getObject(getIndex().intValue() + 1);
		}
		Format fmt = getFormatter();
		if (obj != null && fmt != null) {
			obj = fmt.format(obj);
		}
		return obj;
	}
	
	public void writeTo(PreparedStatement stmt, int idx, Object value) throws SQLException {
		Object toBeWritten = value;
		Format fmt = getFormatter();
		if (toBeWritten != null && fmt != null) {
			try {
				toBeWritten = fmt.parseObject(toBeWritten.toString());
			} catch (ParseException e) {
				throw new IllegalArgumentException("Cannot parse "+value+" as a "+getType());
			}
		}
		stmt.setObject(idx, toBeWritten);
	}

}
