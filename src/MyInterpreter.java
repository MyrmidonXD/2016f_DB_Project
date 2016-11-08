// imports
/* BerkelyDB classes */
import java.io.File;
import java.util.*;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class MyInterpreter {
	
	private static MyInterpreter _instance;
	
	public Environment myDBEnv;
	public Database tableListDB;
	
	private static DatabaseConfig _dbOpenOrCreateCfg;
	private static DatabaseConfig _dbCreateOnlyCfg;
	private static DatabaseConfig _dbOpenOnlyCfg;
	
	static {
		// DatabaseConfigs initiallization.
		_dbOpenOrCreateCfg = new DatabaseConfig();
		_dbOpenOrCreateCfg.setAllowCreate(true);
		_dbCreateOnlyCfg = new DatabaseConfig();
		_dbCreateOnlyCfg.setAllowCreate(true);
		_dbCreateOnlyCfg.setExclusiveCreate(true);
		_dbOpenOnlyCfg = new DatabaseConfig();
	}
	
	// Queues used in Create Table
	private LinkedList<ColumnCreateData> createColumnQueue;
	private LinkedList<PKCreateData> createPKQueue;
	private LinkedList<FKCreateData> createFKQueue;

	private MyInterpreter() {
	    // Open Database Environment or if not exists, create one.
	    EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    myDBEnv = new Environment(new File("db/"), envConfig);
	    
	    // Open Table List Database or if not exists, create one.
	    tableListDB = myDBEnv.openDatabase(null, "SCHEMA_TableList", _dbOpenOrCreateCfg);
	    
	    // Instanciate Create*Queues
	    createColumnQueue = new LinkedList<ColumnCreateData>();
	    createPKQueue = new LinkedList<PKCreateData>();
	    createFKQueue = new LinkedList<FKCreateData>();
	    
	}
	 
	public static MyInterpreter getInstance() {
		if(_instance == null)
			_instance = new MyInterpreter();
		return _instance;
	}
	
	// Top-level interpret methods
	public boolean createTable(String tableName) {
		// TODO implement proper routine handling 'create table'
		return false;
	}
	
	public boolean dropTable() {
		// TODO implement proper routine handling 'drop table'
		return false;
	}
	
	public boolean desc() {
		// TODO implement proper routine handling 'desc'
		return false;
	}
	
	public boolean showTables() {
		// TODO implement proper routine handling 'show tables'
		return false;
	}
	
	// Intermediate nested classes used in interpreting routine
	private class ColumnCreateData {
		public String columnName;
		public DBType columnType;
		public boolean notNull;
		public ColumnCreateData(String colName, String typeStr, int typeVal, boolean isNotNull) {
			columnName = colName;
			notNull = isNotNull;
			if(typeStr.equals("char"))
				columnType = new DBType(typeStr, typeVal);
			else
				columnType = new DBType(typeStr);
		}
	}
	
	private class PKCreateData {
		public ArrayList<String> columnList;
		public PKCreateData(ArrayList<String> colList) {
			columnList = new ArrayList<String>(colList);
		}
	}
	
	private class FKCreateData {
		public ArrayList<String> refingColumnList;
		public String refedTableName;
		public ArrayList<String> refedColumnList;
		public FKCreateData(ArrayList<String> refingColList, String refedTbName, ArrayList<String> refedColList) {
			refingColumnList = new ArrayList<String>(refingColList);
			refedTableName = refedTbName;
			refedColumnList = new ArrayList<String>(refedColList);
		}
	}
	
	// Additional public methods for CREATE TABLE
	public void createTable_Initialize(){
		createColumnQueue.clear();
		createPKQueue.clear();
		createFKQueue.clear();
	}
	
	public void createTable_EnqueueColumn(String colName, String typeStr, int typeVal, boolean isNotNull) {
		ColumnCreateData col = new ColumnCreateData(colName, typeStr, typeVal, isNotNull);
		createColumnQueue.add(col);
	}
	
	public void createTable_EnqueuePK(ArrayList<String> colList) {
		PKCreateData pk = new PKCreateData(colList);
		createPKQueue.add(pk);
	}
	
	public void createTable_EnqueueFK(ArrayList<String> refingColList, String refedTbName, ArrayList<String> refedColList) {
		FKCreateData fk = new FKCreateData(refingColList, refedTbName, refedColList);
		createFKQueue.add(fk);
	}
}
