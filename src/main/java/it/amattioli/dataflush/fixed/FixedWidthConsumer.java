package it.amattioli.dataflush.fixed;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;
import it.amattioli.dataflush.csv.CsvColumn;

public class FixedWidthConsumer extends Consumer {
	private Writer writer;
	private Locale locale = Locale.getDefault();
	private ArrayList columns = new ArrayList();
	
	public Writer getWriter() {
		return writer;
	}

	public void setWriter(Writer writer) {
		this.writer = writer;
	}
	
	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public void setFileName(String fileName) {
		try {
			setWriter(new FileWriter(fileName));
		} catch(IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	public void addColumn(Column column) {
		column.setLocale(getLocale());
		columns.add(column);
	}
	
	public Column createColumn() {
		Column newColumn = new FixedWidthColumn();
		addColumn(newColumn);
		return newColumn;
	}

	public List getColumns() {
		return columns;
	}

	public void consume(Record record) {
		if (record != Record.NULL) {
			writeRecord(record);
		} else {
			IOUtils.closeQuietly(getWriter());
		}
	}

	private void writeRecord(Record record) {
		try {
			StringBuffer values = new StringBuffer(createRecord());
			int idx = 0;
			for (Iterator iter = columns.iterator(); iter.hasNext();) {
				FixedWidthColumn col = (FixedWidthColumn)iter.next();
				try {
					Object val = record.get(idx++);
					col.writeValue(values, val);
				} catch(Exception e) {
					throw new RuntimeException("Error writing column "+col+". "+e.getMessage());
				}
			}
			getWriter().write(values.toString());
			getWriter().write("\n");
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private StringBuffer createRecord() {
		return new StringBuffer(StringUtils.rightPad("", getLength()));
	}

	private int getLength() {
		int result = 0;
		for (Iterator iter = columns.iterator(); iter.hasNext();) {
			FixedWidthColumn curr = (FixedWidthColumn)iter.next();
			if (curr.getEnd() > result) {
				result = curr.getEnd();
			}
		}
		return result;
	}
}
