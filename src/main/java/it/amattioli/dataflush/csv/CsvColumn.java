package it.amattioli.dataflush.csv;

import java.io.IOException;
import java.text.Format;
import java.text.ParseException;

import org.jumpmind.symmetric.csv.CsvReader;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Record;

public class CsvColumn extends Column {

	public CsvColumn() {
		
	}
	
	public CsvColumn(String name) {
		setName(name);
	}
	
	public CsvColumn(Integer index) {
		setIndex(index);
	}
	
	public CsvColumn(String name, Integer index) {
		setName(name);
		setIndex(index);
	}

	public boolean equals(Object obj) {
		if (!(obj instanceof CsvColumn)) {
			return false;
		}
		if (getName() == null) {
			return ((Column)obj).getName() == null;
		}
		return getName().equals(((Column)obj).getName());
	}
	
	public Object getValue(CsvReader csvReader) throws IOException {
		String result;
		if (getName() != null) {
			return parse(csvReader.get(getName()));
		} else {
			return parse(csvReader.get(getIndex().intValue()));
		}
	}
	
	public String getFormattedValue(Record record, int idx) {
		Object val = record.get(idx);
		Format fmt = getFormatter();
		if (fmt != null) {
			try {
				val = fmt.format(val);
			} catch(IllegalArgumentException e) {
				throw new IllegalArgumentException("Cannot format "+val+" as a "+getType());
			}
		}
		return val.toString();
	}

	private Object parse(String value) {
		Format fmt = getFormatter();
		if (fmt != null) {
			try {
				return fmt.parseObject(value);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Cannot parse "+value+" as a "+getType());
			}
		} else {
			return value;
		}
	}
}
