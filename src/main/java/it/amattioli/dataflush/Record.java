package it.amattioli.dataflush;

import java.util.ArrayList;

public class Record {
	public static Record NULL = new Record() {
		public boolean equals(Object other) {
			return this == other;
		}
	};
	
	private ArrayList values = new ArrayList();

	public Object get(int index) {
		return values.get(index);
	}
	
	public void set(int index, Object value) {
		for (int i = values.size(); i <= index; i++) {
			values.add(null);
		}
		values.set(index, value);
	}

	public boolean equals(Object other) {
		if (other == null || !(other instanceof Record)) {
			return false;
		}
		Record otherRecord = (Record)other;
		if (this.values.size() != otherRecord.values.size()) {
			return false;
		}
		for (int idx = 0; idx < values.size(); idx++) {
			if (!values.get(idx).equals(otherRecord.values.get(idx))) {
				return false;
			}
		}
		return true;
	}
	
	public String toString() {
		return values.toString();
	}
}
