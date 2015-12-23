package it.amattioli.dataflush.scripting;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.amattioli.dataflush.text.TextConsumer;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Producer;
import it.amattioli.dataflush.csv.CsvConsumer;
import it.amattioli.dataflush.csv.CsvProducer;
import it.amattioli.dataflush.db.DbConsumer;
import it.amattioli.dataflush.db.DbProducer;
import it.amattioli.dataflush.fixed.FixedWidthColumn;
import it.amattioli.dataflush.fixed.FixedWidthConsumer;
import it.amattioli.dataflush.fixed.FixedWidthProducer;

public class Script {
	static {
		ConvertUtils.register(new Converter() {
			
			public Object convert(Class type, Object value) {
				return new Locale((String)value);
			}

		}, Locale.class);
	}
	private Producer producer;
	private Consumer consumer;
	private Object current;
	private int columnIndex = 0;
	
	public Script() {

	}
	
	public Script(File file) throws FileNotFoundException {
		this(new FileReader(file));
	}
	
	public Script(Reader reader) {
		int lineNumber = 1;
		for (LineIterator iter = IOUtils.lineIterator(reader); iter.hasNext();) {
			String line = iter.nextLine().trim();
			try {
				processLine(line);
			} catch(Exception e) {
				throw new ScriptException(lineNumber, e);
			}
			lineNumber++;
		}
	}

	protected void processLine(String line) {
		if (line.startsWith("#")) {
			// Ignore comments
		} else if (line.startsWith("FROM")) {
			addProducer(line);
		} else if (line.startsWith("TO")) {
			addConsumer(line);
		} else if (line.startsWith("COLUMN")) {
			addColumn(line);
		} else {
			if (current == null) {
				throw new IllegalStateException("No FROM, TO or COLUMN section started");
			}
			setAttribute(current, line);
		}
	}

	protected void addColumn(String line) {
		Pattern p = Pattern.compile("COLUMN\\s+(.*)\\s+=>\\s+(.*)");
		Matcher matcher = p.matcher(line);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("Wrong column definition");
		}
		String producerCol = matcher.group(1);
		String consumerCol = matcher.group(2);
		addProducerCol(producerCol);
		addConsumerCol(consumerCol);
		columnIndex++;
	}

	protected void addConsumerCol(String colDefinition) {
		Column col = consumer.createColumn();
		parseColumnDefinition(colDefinition, col);
	}
	
	protected void addProducerCol(String colDefinition) {
		Column col = producer.createColumn();
		parseColumnDefinition(colDefinition, col);
	}

	private void parseColumnDefinition(String colDefinition, Column col) {
		String[] elements = colDefinition.split(" ");
		col.setIndex(new Integer(columnIndex));
		int idx = 0;
		while (elements[idx] == null || elements[idx].trim().equals("")) {
			idx++;
		}
		if (elements[idx].matches("\\(\\d+\\)")) {
			col.setIndex(Integer.valueOf(elements[idx].substring(1, elements[idx].length()-1)));
		} else if (elements[idx].matches("\\[\\d+,\\d+\\]")) {
			Matcher matcher = Pattern.compile("\\[(\\d+),(\\d+)\\]").matcher(elements[idx]);
			matcher.matches();
			((FixedWidthColumn)col).setStart(Integer.parseInt(matcher.group(1)));
			((FixedWidthColumn)col).setEnd(Integer.parseInt(matcher.group(2)));
		} else {
			col.setName(elements[idx].trim());
		}
		for (int i = idx + 1; i < elements.length; i++) {
			if (elements[i] != null && !elements[i].trim().equals("")) {
				setAttribute(col, elements[i]);
			}
		}
	}
	
	protected void addConsumer(String line) {
		consumer = createConsumer(line);
		current = consumer;
	}
	
	protected void addProducer(String line) {
		producer = createProducer(line);
		current = producer;
	}

	protected Consumer createConsumer(String line) {
		if (line.endsWith("CSV")) {
			return new CsvConsumer();
		} else if (line.endsWith("DATABASE")) {
			return new DbConsumer();
		} else if (line.endsWith("TEXT")) {
			return new TextConsumer();
		} else if (line.endsWith("FIXED WIDTH")) {
			return new FixedWidthConsumer();
		} else {
			throw new IllegalArgumentException("Unknown destination");
		}
	}

	protected Producer createProducer(String line) {
		if (line.endsWith("CSV")) {
			return new CsvProducer();
		} else if (line.endsWith("DATABASE")) {
			return new DbProducer();
		} else if (line.endsWith("FIXED WIDTH")) {
			return new FixedWidthProducer();
		} else {
			throw new IllegalArgumentException("Unknown source");
		}
	}
	
	protected void setAttribute(Object bean, String line) {
		Pattern p = Pattern.compile("(\\S*)\\s*:\\s*(.*)");
		Matcher matcher = p.matcher(line);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("Cannot understand '" + line + "'");
		}
		String name = matcher.group(1);
		String value = matcher.group(2);
		try {
			BeanUtils.setProperty(bean, name, value);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		} catch (InvocationTargetException e) {
			throw new IllegalArgumentException(e.getCause());
		}
	}
	
	public Producer getProducer() {
		return producer;
	}
	
	public Consumer getConsumer() {
		return consumer;
	}

}
