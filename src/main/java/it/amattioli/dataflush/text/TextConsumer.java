package it.amattioli.dataflush.text;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Consumer;
import it.amattioli.dataflush.Record;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * Created by andrea on 23/12/2015.
 */
public class TextConsumer extends Consumer {
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
        Column newColumn = new TextColumn();
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
            try {
                writer.close();
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void writeRecord(Record record) {
        try {
            String[] values = new String[columns.size()];
            int idx = 0;
            for (Iterator iter = columns.iterator(); iter.hasNext();) {
                TextColumn col = (TextColumn)iter.next();
                try {
                    values[col.getIndex().intValue()] = col.getFormattedValue(record, idx++); //record.get(idx++).toString();
                } catch(Exception e) {
                    throw new RuntimeException("Error writing column "+col+". "+e.getMessage());
                }
            }
            writeValues(values);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeValues(String[] values) throws IOException {
        for (int i = 0; i < values.length; i++) {
            writer.write(values[i]);
        }
    }
}
