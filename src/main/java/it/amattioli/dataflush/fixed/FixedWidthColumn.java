package it.amattioli.dataflush.fixed;

import java.text.Format;
import java.text.ParseException;

import org.apache.commons.lang3.StringUtils;

import it.amattioli.dataflush.Column;

public class FixedWidthColumn extends Column {
	private int start,end;
	
	public FixedWidthColumn() {
		
	}
	
	public FixedWidthColumn(int start, int end) {
		setStart(start);
		setEnd(end);
	}

	public int getStart() {
		return start;
	}
	
	public void setStart(int start) {
		this.start = start;
	}
	
	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end + 1;
	}

	public int getLength() {
		return getEnd() - getStart();
	}
	
	public boolean equals(Object obj) {
		if (!(obj instanceof FixedWidthColumn)) {
			return false;
		}
		FixedWidthColumn other = (FixedWidthColumn)obj;
		return other.start == start && other.end == end;
	}
	
	public Object getValue(String line) {
		return parse(line.substring(start, end));
	}
	
	public String getFormattedValue(Object val) {
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
	
	public void writeValue(StringBuffer line, Object value) {
		String fmtVal = getFormattedValue(value);
		if (fmtVal.length() > getLength()) {
			fmtVal = fmtVal.substring(0, getLength());
		}
		if (fmtVal.length() < getLength()) {
			fmtVal = StringUtils.rightPad(fmtVal, getLength());
		}
		line.replace(getStart(), getEnd(), fmtVal);
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
