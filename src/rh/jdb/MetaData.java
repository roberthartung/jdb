package rh.jdb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MetaData {
	private RandomAccessFile file;
	
	private static final int METADATA_TABLE = 0x01;
	
	private static final int METADATA_COLUMN = 0x02;
	
	private Boolean metaDataOutOfDate = null;
	
	private final static String CHARSET = "UTF-8";
	
	private Map<String, Table> tables = new HashMap<String, Table>();
	
	protected MetaData(String path) throws JDBException {
		try {
			file = new RandomAccessFile(path, "rw");
		} catch (FileNotFoundException e) {
			throw new JDBException("Unable to open metadata");
		}
		
		/*
		try {
			if(file.length() == 0) {
				// No MetaData available
			} else {
				System.out.println(file.read());
			}
		} catch (IOException e1) {
			throw new JDBException("Unable to read metadata");
		}
		*/
		
		checkMetaData();
	}
	
	private void checkMetaData() throws JDBException {
		if(metaDataOutOfDate == null) {
			int b;
			Table t;
			try {
				while((b = file.read()) != -1) {
					switch(b) {
					case METADATA_TABLE : 
							int length = file.read();
							String name = new String(readBytes(length), CHARSET);
							t = new Table(name);
							tables.put(name, t);
							// TODO(rh): read columns
						break;
					case METADATA_COLUMN :
						// t.addColumn(name, type);
						break;
						default :
							throw new JDBException("MetaData is malicious.");
					}
				}
			} catch (IOException e) {
				throw new JDBException("Unable to read metadata");
			}
		}
	}
	
	private byte[] readBytes(int length) throws IOException {
		byte[] data = new byte[length];
		int offset = 0;
		while(length - offset > 0) {
			offset += file.read(data, offset, length - offset);
		}
		return data;
	}
	
	public void addTable(Table table) throws JDBException {
		checkMetaData();
		
		if(tables.containsKey(table.getName())) {
			throw new JDBException("Table already exists.");
		}
		
		try {
			synchronized(this) {
				long pos = file.getFilePointer();
				file.seek(file.length());
				byte[] name = table.getName().getBytes(CHARSET);
				// ByteBuffer buffer = ByteBuffer.allocate(1+name.length);
				file.write(METADATA_TABLE);
				file.write(name.length);
				file.write(name);
				// file.write(buffer.array());
				file.seek(pos);
			}
		} catch(IOException e) {
			
		}
	}
}
