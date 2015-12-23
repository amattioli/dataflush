package it.amattioli.dataflush.text;

import it.amattioli.dataflush.Column;
import it.amattioli.dataflush.Record;

import java.text.Format;

/**
 * Created by andrea on 23/12/2015.
 */
public class TextColumn extends Column {
    public TextColumn() {

    }

    public TextColumn(String name) {
        setName(name);
    }

    public TextColumn(Integer index) {
        setIndex(index);
    }

    public TextColumn(String name, Integer index) {
        setName(name);
        setIndex(index);
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof TextColumn)) {
            return false;
        }
        if (getName() == null) {
            return ((Column)obj).getName() == null;
        }
        return getName().equals(((Column)obj).getName());
    }
    /*
    public Object getValue(CsvReader csvReader) throws IOException {
        String result;
        if (getName() != null) {
            return parse(csvReader.get(getName()));
        } else {
            return parse(csvReader.get(getIndex().intValue()));
        }
    }
    */
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
    /*
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
    */
}
