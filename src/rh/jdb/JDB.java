package rh.jdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class JDB {
	private String root_path;

	private File root_directory;
	
	private MetaData metadata;
	
	public JDB(String root) throws JDBException {
		this.root_path = root;
		root_directory = new File(root);
		if(!root_directory.isDirectory() || !root_directory.exists()) {
			if(!root_directory.mkdir()) {
				throw new JDBException("Unable to create data directory.");
			}
		}
		
		metadata = new MetaData(root_directory.getPath() + "/metadata.jdb");
	}
	
	Boolean metaDataChanged = null;
	
	public Table createTable(String name) throws JDBException {
		Table table = new Table(name);
		metadata.addTable(table);
		return table;
	}
	
	public Table createTable(String name, Map<String,Class<?>> columns) throws JDBException {
		return createTable(name);
	}
}
