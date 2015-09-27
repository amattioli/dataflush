package it.amattioli.dataflush;

import java.util.List;

public abstract class Consumer {
	
	public abstract Column createColumn();

	public abstract void addColumn(Column column);
	
	public abstract List getColumns();
	
	public abstract void consume(Record record);
	
}
