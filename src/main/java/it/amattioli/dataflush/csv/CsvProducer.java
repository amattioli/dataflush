package it.amattioli.dataflush.csv;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.jumpmind.symmetric.csv.CsvReader;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Producer;
import it.amattioli.dataflush.Record;

public class CsvProducer extends Producer {
	private Consumer consumer;
	private boolean headers = false;
	private Reader reader;
	private Locale locale = Locale.getDefault();
	private ArrayList columns = new ArrayList();
	private char delimiter = ';';

	public Consumer getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}

	public boolean isHeaders() {
		return headers;
	}

	public void setHeaders(boolean headers) {
		this.headers = headers;
	}

	public Reader getReader() {
		return reader;
	}

	public void setReader(Reader reader) {
		this.reader = reader;
	}
	
	public void setFileName(String fileName) {
		try {
			setReader(new FileReader(fileName));
		} catch(FileNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
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
		Column newColumn = new CsvColumn();
		addColumn(newColumn);
		return newColumn;
	}
	
	public List getColumns() {
		return columns;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}

	public void produce() {
		CsvReader csvReader = new CsvReader(reader, delimiter);
		try {
			if (headers) {
				csvReader.readHeaders();
			}
			while (csvReader.readRecord()) {
				Record record;
				record = createRecord(csvReader);
				consumer.consume(record);
			}
		} catch(IOException e) {
			throw new RuntimeException(e);
		} finally {
			consumer.consume(Record.NULL);
			csvReader.close();
		}
	}

	private Record createRecord(CsvReader csvReader) throws IOException {
		Record record = new Record();
		int index = 0;
		for (Iterator iter = columns.iterator(); iter.hasNext();) {
			CsvColumn column = (CsvColumn)iter.next();
			Object value;
			try {
				value = column.getValue(csvReader);
			} catch(Exception e) {
				throw new RuntimeException("Error reading column "+column+". "+e.getMessage());
			}
			record.set(index++, value);
		}
		return record;
	}

}
