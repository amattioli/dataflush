package it.amattioli.dataflush.fixed;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Producer;
import it.amattioli.dataflush.Record;

public class FixedWidthProducer extends Producer {
	private Consumer consumer;
	private Reader reader;
	private Locale locale = Locale.getDefault();
	private ArrayList columns = new ArrayList();
	
	public Consumer getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
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
	
	public Column createColumn() {
		Column newColumn = new FixedWidthColumn();
		addColumn(newColumn);
		return newColumn;
	}

	public void addColumn(Column column) {
		column.setLocale(getLocale());
		columns.add(column);
	}

	public List getColumns() {
		return columns;
	}
	
	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public void produce() {
		LineIterator it = IOUtils.lineIterator(getReader());
		try {
		   while (it.hasNext()) {
		     String line = it.nextLine();
		     Record record;
			record = createRecord(line);
			consumer.consume(record);
		   }
		} catch(IOException e) {
			throw new RuntimeException(e);
		} finally {
			consumer.consume(Record.NULL);
		   LineIterator.closeQuietly(it);
		}
	}
	
	private Record createRecord(String line) throws IOException {
		Record record = new Record();
		int index = 0;
		for (Iterator iter = getColumns().iterator(); iter.hasNext();) {
			FixedWidthColumn column = (FixedWidthColumn)iter.next();
			Object value;
			try {
				value = column.getValue(line);
			} catch(Exception e) {
				throw new RuntimeException("Error reading column "+column+". "+e.getMessage());
			}
			record.set(index++, value);
		}
		return record;
	}
	
}
