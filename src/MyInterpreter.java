// imports
/* BerkelyDB classes */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class MyInterpreter {
	
	private static MyInterpreter _instance;
	
	public Environment myDBEnv;
	
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
	public void createTable(String tableName) throws DBError {
		// Open SCHEMA_TableList for CREATE TABLE
		Database tableListDB = myDBEnv.openDatabase(null, "SCHEMA_TableList", _dbOpenOrCreateCfg);
		try {
			DatabaseEntry newTableName = new DatabaseEntry(tableName.getBytes("UTF-8"));
			DatabaseEntry tmp = new DatabaseEntry(); // used for membership test with Database.get()
			
			HashMap<String, ColumnCreateData> columnMap = new HashMap<String, ColumnCreateData>(); // Stores column name and its ColumnCreateData;
			
			// Validation 1 - Check TableExistenceError
			if(tableListDB.get(null, newTableName, tmp, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				throw new TableExistenceError();				
			}
			
			// Validation 2 - Check DuplicateColumnDefError & CharLengthError
			for(ColumnCreateData cd : createColumnQueue) {
				if(columnMap.containsKey(cd.columnName))
					throw new DuplicateColumnDefError();
				if(cd.columnType.type == DBType.DBTypeSpecifier.DB_CHAR && cd.columnType.length < 1)
					throw new CharLengthError();
				columnMap.put(cd.columnName, cd); // For Later use
			}
			
			// Validation 3 - Check DuplicatePrimaryKeyDefError
			if(createPKQueue.size() > 1)
				throw new DuplicatePrimaryKeyDefError();
			
			// Validation 4 - Check NonExistingColumnDefError for PK Definition
			if(createPKQueue.size() == 1) {
				PKCreateData pkd = createPKQueue.get(0);
				for(String col : pkd.columnList) {
					if(!columnMap.containsKey(col))
						throw new NonExistingColumnDefError(col);
				}
			}
			
			// Validation 5 ~ 9 - About foreign keys
			int idx = 0;
			for(FKCreateData fkd : createFKQueue) {
				// Validation 5 - Check NonExistingColumnDefError for FK Definition & DuplicateForeignKeyDefError
				for(String col : fkd.refingColumnList) {
					if(!columnMap.containsKey(col))
						throw new NonExistingColumnDefError(col);
				}
				for(int i = 0; i < idx; i++) {
					boolean sameFK = true;
					FKCreateData prevFK = createFKQueue.get(i);
					if(fkd.refingColumnList.size() != prevFK.refingColumnList.size()) continue;
					for(String col : fkd.refingColumnList) {
						sameFK = sameFK && prevFK.refingColumnList.contains(col);
						// if prevFK's referencing column list doesn't contain a col, then sameFK must be false in the end.
					}
					
					if(sameFK) 
						throw new DuplicateForeignKeyDefError();
				}
				
				// Validation 6 - Check ReferenceTableExistenceError & ReferenceOwnTableError
				if(fkd.refedTableName.equals(tableName))
					throw new ReferenceOwnTableError();
				DatabaseEntry refedTableNameKey = new DatabaseEntry(fkd.refedTableName.getBytes("UTF-8"));
				if(tableListDB.get(null, refedTableNameKey, tmp, LockMode.DEFAULT) == OperationStatus.NOTFOUND)
					throw new ReferenceTableExistenceError();
				
				// Validation 7 ~ 9 - Check ReferenceColumnExistenceError, ReferenceNonPrimaryKeyError, ReferenceTypeError
				try {
					Database refedTableColumnDB = myDBEnv.openDatabase(null, "SCHEMA_COLUMN_"+fkd.refedTableName, _dbOpenOnlyCfg);
					
					// Validation 7 - Check ReferenceColumnExistenceError
					for(String col : fkd.refedColumnList) {
						DatabaseEntry fkColNameKey = new DatabaseEntry(col.getBytes("UTF-8"));
						if(refedTableColumnDB.get(null, fkColNameKey, tmp, LockMode.DEFAULT) == OperationStatus.NOTFOUND)
							throw new ReferenceColumnExistenceError();
					}
					
					// Validation 8 - Check ReferenceNonPrimaryKeyError
					DatabaseEntry resultData = new DatabaseEntry();
					if(tableListDB.get(null, refedTableNameKey, resultData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
						TableListDBEntry refedTableData = (TableListDBEntry)MyInterpreter.fromBytes(resultData.getData());
						if(fkd.refedColumnList.size() != refedTableData.pkColumnList.size())
							throw new ReferenceNonPrimaryKeyError();
						for(String col : fkd.refedColumnList) {
							if(!refedTableData.pkColumnList.contains(col))
								throw new ReferenceNonPrimaryKeyError();
						}
					}
					else {
						throw new RuntimeException("Referenced table schema in SCHEMA_TableList access failed!!");
					}
					
					// Validation 9 - Check ReferenceTypeError
					if(fkd.refingColumnList.size() != fkd.refedColumnList.size())
						throw new ReferenceTypeError();
					for(int i = 0 ; i < fkd.refingColumnList.size(); i++) {
						DBType refingColType = columnMap.get((fkd.refingColumnList.get(i))).columnType;
						DBType refedColType;
						
						DatabaseEntry fkColNameKey = new DatabaseEntry(fkd.refedColumnList.get(i).getBytes("UTF-8"));
						if(refedTableColumnDB.get(null, fkColNameKey, resultData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
							ColumnListDBEntry refedColData = (ColumnListDBEntry)MyInterpreter.fromBytes(resultData.getData());
							refedColType = refedColData.columnType;
							if(!refingColType.equals(refedColType))
								throw new ReferenceTypeError();
						}
						else {
							throw new RuntimeException("Referenced column schema in SCHEMA_COLUMN_" + fkd.refedTableName + " access failed!!");
						}
					}
					
					refedTableColumnDB.close();
				}
				catch(DatabaseNotFoundException e) {
					// if referenced table's column database is not exist
					// in normal situation, this will not happen
					throw new ReferenceColumnExistenceError(); 			
				}
				idx++;
			}
			
			// ----- If code reaches here, then there is no problem to create this table! -----------------------------------------
			ArrayList<String> pkColList;
			if(createPKQueue.size() == 1)
				pkColList = createPKQueue.get(0).columnList;
			else
				pkColList = new ArrayList<String>();
			
			TableListDBEntry tableEntry = new TableListDBEntry(tableName, pkColList);
			
			// Force not null constraint to pk columns
			for (String col : pkColList) {
				columnMap.get(col).notNull = true;
			}
			
			// Insert table entry in SCHEMA_TableList DB.
			DatabaseEntry newTableEntry = new DatabaseEntry(MyInterpreter.toBytes(tableEntry));
			if(tableListDB.put(null, newTableName, newTableEntry) != OperationStatus.SUCCESS) {
				throw new RuntimeException("Inserting the new table entry in SCHEMA_TableList failed!!");
			}
			
			// Make a set which contains columns attending in a foreign key.
			HashSet<String> fkColumnSet = new HashSet<String>();
			for(FKCreateData fk : createFKQueue) {
				for(String col : fk.refingColumnList)
					fkColumnSet.add(col);
			}
			
			// Insert Columns in SCHEMA_COLUMN_<table name> DB
			Database newColumnDB = myDBEnv.openDatabase(null, "SCHEMA_COLUMN_" + tableName, _dbCreateOnlyCfg);
			int colIdx = 0;
			while(createColumnQueue.size() > 0) {
				ColumnCreateData newCol = createColumnQueue.poll();
				ColumnListDBEntry newColDBEntry = new ColumnListDBEntry(newCol.columnName, newCol.columnType, colIdx, !newCol.notNull, pkColList.contains(newCol.columnName), fkColumnSet.contains(newCol.columnName));
				DatabaseEntry newColNameKey = new DatabaseEntry(newCol.columnName.getBytes("UTF-8"));
				DatabaseEntry newColEntry = new DatabaseEntry(MyInterpreter.toBytes(newColDBEntry));
				if(newColumnDB.put(null, newColNameKey, newColEntry) != OperationStatus.SUCCESS) {
					throw new RuntimeException("Inserting the new column entry in SCHEMA_COLUMN_" + tableName + " failed!!");
				}
				colIdx++;
			}
			newColumnDB.close();
			
			// Insert Foreign Keys in SCHEMA_FOREIGNKEY_<table name> DB
			Database newForeignKeyDB = myDBEnv.openDatabase(null, "SCHEMA_FOREIGNKEY_" + tableName, _dbCreateOnlyCfg);
			while(createFKQueue.size() > 0) {
				FKCreateData newFk = createFKQueue.poll();
				ForeignKeyListDBEntry newFkDBEntry = new ForeignKeyListDBEntry(newFk.refingColumnList, newFk.refedTableName, newFk.refedColumnList);
				DatabaseEntry newFkRefingKey = new DatabaseEntry(MyInterpreter.toBytes(newFk.refingColumnList));
				DatabaseEntry newFkEntry = new DatabaseEntry(MyInterpreter.toBytes(newFkDBEntry));
				if(newForeignKeyDB.put(null, newFkRefingKey, newFkEntry) != OperationStatus.SUCCESS) {
					throw new RuntimeException("Inserting the new foreign key entry in SCHEMA_FOREIGNKEY_" + tableName + " failed!!");
				}
				
				// Update the refCount in tableListDB
				DatabaseEntry refedTableNameKey = new DatabaseEntry(newFk.refedTableName.getBytes("UTF-8"));
				DatabaseEntry refedTableEntry = new DatabaseEntry();
				if(tableListDB.get(null, refedTableNameKey, refedTableEntry, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
					throw new RuntimeException("Accessing the referenced table entry in SCHEMA_TableList failed!!");
				}
				TableListDBEntry refedTableDBEntry = (TableListDBEntry)MyInterpreter.fromBytes(refedTableEntry.getData());
				tableListDB.delete(null, refedTableNameKey);
				refedTableDBEntry.refCount++;
				refedTableEntry = new DatabaseEntry(MyInterpreter.toBytes(refedTableDBEntry));
				if(tableListDB.put(null, refedTableNameKey, refedTableEntry) != OperationStatus.SUCCESS) {
					throw new RuntimeException("Updating the referenced table entry in SCHEMA_TableList failed!!");
				}
			}
			newForeignKeyDB.close();
			
			tableListDB.close();
			System.out.println("\'" + tableName + "\' table is created");
		}
		catch (DBError e) {
			tableListDB.close();
			throw e;
		}
		catch (UnsupportedEncodingException e) {
			tableListDB.close();
			e.printStackTrace();
		}
	}
	
	public void dropTable(String tableName) throws DBError {
		Database tableListDB = myDBEnv.openDatabase(null, "SCHEMA_TableList", _dbOpenOrCreateCfg);
		try {
			DatabaseEntry tableNameKey = new DatabaseEntry(tableName.getBytes("UTF-8"));
			DatabaseEntry tableDBEntry = new DatabaseEntry();
			
			if(tableListDB.get(null, tableNameKey, tableDBEntry, LockMode.DEFAULT) == OperationStatus.NOTFOUND) {
				throw new NoSuchTable();
			}
			
			TableListDBEntry tableEntry = (TableListDBEntry)MyInterpreter.fromBytes(tableDBEntry.getData());
			if(tableEntry.refCount > 0) {
				throw new DropReferencedTableError(tableName);
			}
			
			// Decreasing tables' refCount referecned by this table 
			Database tableForeignKeyDB = myDBEnv.openDatabase(null, "SCHEMA_FOREIGNKEY_"+tableName, _dbOpenOnlyCfg);
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			Cursor fkcursor = tableForeignKeyDB.openCursor(null, null);
			
			fkcursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
			
			if(tableForeignKeyDB.count() > 0) {
				do {
					FKCreateData fkd = (FKCreateData)MyInterpreter.fromBytes(foundData.getData());
					DatabaseEntry refedTableNameKey = new DatabaseEntry(fkd.refedTableName.getBytes("UTF-8"));
					DatabaseEntry refedTableDBEntry = new DatabaseEntry();
					
					tableListDB.get(null, refedTableNameKey, refedTableDBEntry, LockMode.DEFAULT);
					TableListDBEntry refedTableEntry = (TableListDBEntry)MyInterpreter.fromBytes(refedTableDBEntry.getData());
					refedTableEntry.refCount--;
					tableListDB.delete(null, refedTableNameKey);
					refedTableDBEntry = new DatabaseEntry(MyInterpreter.toBytes(refedTableEntry));
					tableListDB.put(null, refedTableNameKey, refedTableDBEntry);
				}
				while(fkcursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
			}
			
			fkcursor.close();
			tableForeignKeyDB.close();
			tableListDB.delete(null, tableNameKey);
			myDBEnv.removeDatabase(null, "SCHEMA_COLUMN_"+tableName);
			myDBEnv.removeDatabase(null, "SCHEMA_FOREIGNKEY_"+tableName);
			
			System.out.println("\'"+tableName+"\' table is dropped");
		}
		catch(DBError e) {
			throw e;
		}
		catch(UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		finally {
			tableListDB.close();
		}
	}
	
	public void desc(String tableName) throws DBError {
		Database columnDB;
		try {
			 columnDB = myDBEnv.openDatabase(null, "SCHEMA_COLUMN_"+tableName, _dbOpenOnlyCfg);
			 DatabaseEntry foundKey = new DatabaseEntry();
			 DatabaseEntry foundData = new DatabaseEntry();
			 
			 Cursor colCursor = columnDB.openCursor(null, null);
			 
			 colCursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
			 
			 System.out.println("-------------------------------------------------");
			 System.out.println("table_name [" + tableName + "]");
			 System.out.printf("%-25s%-15s%-15s%-15s\n", "column_name", "type", "null", "key");
			 
			 do {
				 ColumnListDBEntry colDBEntry = (ColumnListDBEntry)MyInterpreter.fromBytes(foundData.getData());
				 String isNullable = (colDBEntry.nullable) ? "Y" : "N";
				 String keyType = "";
				 if(colDBEntry.primaryKey) {
					 if(colDBEntry.foreignKey) keyType = "PRI/FOR";
					 else keyType = "PRI";
				 }
				 else if(colDBEntry.foreignKey) keyType = "FOR";
				 
				 System.out.printf("%-25s%-15s%-15s%-15s\n", colDBEntry.columnName, colDBEntry.columnType.toString(), isNullable, keyType);
			 }
			 while(colCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
			 
			 System.out.println("-------------------------------------------------");
			 
			 colCursor.close();
			 columnDB.close();
		}
		catch(DatabaseNotFoundException e) {
			throw new NoSuchTable();
		}
	}
	
	public void showTables() throws DBError{
		Database tableListDB = myDBEnv.openDatabase(null, "SCHEMA_TableList", _dbOpenOrCreateCfg);
		try {
			if(tableListDB.count() <= 0)
				throw new ShowTablesNoTable();
			
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			 
			Cursor tableCursor = tableListDB.openCursor(null, null);
			 
			tableCursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
			
			System.out.println("----------------");
			
			do {
				 String tableName = new String(foundKey.getData(), "UTF-8");
				 System.out.println(tableName);
			}
			while(tableCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
			
			System.out.println("----------------");
			
			tableCursor.close();
		}
		catch(DBError e) {
			throw e;
		}
		catch(UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		finally {
			tableListDB.close();
		}
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
	
	public static byte[] toBytes(Serializable o) {
		try {
			ByteArrayOutputStream bOut = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(bOut);
			out.writeObject(o);
			return bOut.toByteArray();
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static Object fromBytes(byte[] ba) {
		try {
			ByteArrayInputStream bIn = new ByteArrayInputStream(ba);
			ObjectInputStream in = new ObjectInputStream(bIn);
			return in.readObject();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void terminate() {
		if(myDBEnv != null) myDBEnv.close();
	}
}
