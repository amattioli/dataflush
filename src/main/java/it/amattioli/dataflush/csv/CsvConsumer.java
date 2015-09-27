package it.amattioli.dataflush.csv;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.jumpmind.symmetric.csv.CsvWriter;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

public class CsvConsumer extends Consumer {
	private boolean headers = false;
	private char delimiter = ';';
	private Writer writer;
	private Locale locale = Locale.getDefault();
	private ArrayList columns = new ArrayList();
	private CsvWriter csvWriter;
	
	public boolean isHeaders() {
		return headers;
	}

	public void setHeaders(boolean headers) {
		this.headers = headers;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}

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
		Column newColumn = new CsvColumn();
		addColumn(newColumn);
		return newColumn;
	}
	
	public List getColumns() {
		return columns;
	}

	private CsvWriter getCsvWriter() {
		if (csvWriter == null) {
			csvWriter = new CsvWriter(writer, delimiter);
			if (headers) {
				try {
					String[] values = new String[columns.size()];
					for (Iterator iter = columns.iterator(); iter.hasNext();) {
						CsvColumn col = (CsvColumn)iter.next();
						values[col.getIndex().intValue()] = col.getName();
						//csvWriter.write(col.getName());
					}
					writeValues(values);
					csvWriter.endRecord();
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return csvWriter;
	}

	public void consume(Record record) {
		if (record != Record.NULL) {
			writeRecord(record);
		} else {
			getCsvWriter().close();
		}
	}

	private void writeRecord(Record record) {
		try {
			String[] values = new String[columns.size()];
			int idx = 0;
			for (Iterator iter = columns.iterator(); iter.hasNext();) {
				CsvColumn col = (CsvColumn)iter.next();
				try {
					values[col.getIndex().intValue()] = col.getFormattedValue(record, idx++); //record.get(idx++).toString();
				} catch(Exception e) {
					throw new RuntimeException("Error writing column "+col+". "+e.getMessage());
				}
			}
			writeValues(values);
			getCsvWriter().endRecord();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void writeValues(String[] values) throws IOException {
		for (int i = 0; i < values.length; i++) {
			getCsvWriter().write(values[i]);
		}
	}

}
