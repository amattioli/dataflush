package it.amattioli.dataflush;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Column {
	private String name;
	private Integer index;
	private String type;
	private String format;
	private Locale locale = Locale.getDefault();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getIndex() {
		return index;
	}

	public void setIndex(Integer index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}
	
	public Locale getLocale() {
		return locale;
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public Format getFormatter() {
		if (getType() == null || getFormat() == null || getType().equalsIgnoreCase("string")) {
			return null;
		} else if (getType().equalsIgnoreCase("number")) {
			return new DecimalFormat(getFormat(), DecimalFormatSymbols.getInstance(locale));
		} else if (getType().equalsIgnoreCase("date")) {
			return new SimpleDateFormat(getFormat());
		} else {
			throw new IllegalStateException("Unknown type: "+getType());
		}
	}

	public String toString() {
		if (getName() != null) {
			return getName();
		} else if (getIndex() != null) {
			return "(" + getIndex() + ")";
		} else {
			return "";
		}
	}
}
