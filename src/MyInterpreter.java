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
						if(refedTableColumnDB.get(null, fkColNameKey, tmp, LockMode.DEFAULT) == OperationStatus.NOTFOUND) {
							refedTableColumnDB.close();
							throw new ReferenceColumnExistenceError();
						}
					}
					
					// Validation 8 - Check ReferenceNonPrimaryKeyError
					DatabaseEntry resultData = new DatabaseEntry();
					if(tableListDB.get(null, refedTableNameKey, resultData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
						TableListDBEntry refedTableData = (TableListDBEntry)MyInterpreter.fromBytes(resultData.getData());
						if(fkd.refedColumnList.size() != refedTableData.pkColumnList.size()) {
							refedTableColumnDB.close();
							throw new ReferenceNonPrimaryKeyError();
						}
						for(String col : fkd.refedColumnList) {
							if(!refedTableData.pkColumnList.contains(col)) {
								refedTableColumnDB.close();
								throw new ReferenceNonPrimaryKeyError();
							}
						}
					}
					else {
						throw new RuntimeException("Referenced table schema in SCHEMA_TableList access failed!!");
					}
					
					// Validation 9 - Check ReferenceTypeError
					if(fkd.refingColumnList.size() != fkd.refedColumnList.size()) {
						refedTableColumnDB.close();
						throw new ReferenceTypeError();
					}
					for(int i = 0 ; i < fkd.refingColumnList.size(); i++) {
						DBType refingColType = columnMap.get((fkd.refingColumnList.get(i))).columnType;
						DBType refedColType;
						
						DatabaseEntry fkColNameKey = new DatabaseEntry(fkd.refedColumnList.get(i).getBytes("UTF-8"));
						if(refedTableColumnDB.get(null, fkColNameKey, resultData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
							ColumnListDBEntry refedColData = (ColumnListDBEntry)MyInterpreter.fromBytes(resultData.getData());
							refedColType = refedColData.columnType;
							if(!refingColType.equals(refedColType)) {
								refedTableColumnDB.close();
								throw new ReferenceTypeError();
							}
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
					ForeignKeyListDBEntry fkd = (ForeignKeyListDBEntry)MyInterpreter.fromBytes(foundData.getData());
					DatabaseEntry refedTableNameKey = new DatabaseEntry(fkd.referencedTableName.getBytes("UTF-8"));
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
	
	public void insertInto(String tableName, ArrayList<String> colList, ArrayList<DBValue> valList) throws DBError {
		// Open SCHEMA_TableList for INSERT INTO
		Database tableListDB = myDBEnv.openDatabase(null, "SCHEMA_TableList", _dbOpenOrCreateCfg);
		try 
		{
			DatabaseEntry tableNameKey = new DatabaseEntry(tableName.getBytes("UTF-8"));
			DatabaseEntry tableDBEntry = new DatabaseEntry();
			
			if(tableListDB.get(null, tableNameKey, tableDBEntry, LockMode.DEFAULT) == OperationStatus.NOTFOUND) {
				throw new NoSuchTable();
			}
			
			ArrayList<ColumnListDBEntry> colSchema = getColumnSchema(tableName);
			
			// Checks the number of columns
			if(colList.size() == 0) {
				if(valList.size() != colSchema.size()) throw new InsertTypeMismatchError();
			}
			else {
				if(valList.size() != colList.size()) throw new InsertTypeMismatchError();
			}
			
			// Make a map for column name and its schema
			HashMap<String, ColumnListDBEntry> colSchemaMap = new HashMap<String, ColumnListDBEntry>();
			for(ColumnListDBEntry c : colSchema) {
				colSchemaMap.put(c.columnName, c);
			}
			
			// Make a map for column name and its DBValue (map between colList and valList)
			HashMap<String, DBValue> colListMap = new HashMap<String, DBValue>();
			for(int i = 0; i < colList.size(); i++) {
				String currColName = colList.get(i);
				if(colListMap.containsKey(currColName) == false && colSchemaMap.containsKey(currColName) == true)
					colListMap.put(colList.get(i), valList.get(i));
				else if (colSchemaMap.containsKey(currColName) == false) 
					throw new InsertColumnExistenceError(currColName);
				else
					throw new RuntimeException("Unexpected Error: Column names are duplicated in insert clause!");
			}
			
			// Make a record to be inserted
			ArrayList<DBValue> record = new ArrayList<DBValue>();
			if(colList.size() == 0) {
				record.addAll(valList);
			}
			else {
				for(int i = 0; i < colSchema.size(); i++) {
					String currColName = colSchema.get(i).columnName;
					if(colListMap.containsKey(currColName))
						record.add(colListMap.get(currColName));
					else
						record.add(new DBValue()); // Insert NULL for columns not specified in colList.
				}
			}
			
			// Check the record has some problems
			
			// Validation 1. Type Matching & Non Null Constraint
			for(int i = 0; i < colSchema.size(); i++) {
				DBValue currValue = record.get(i);
				ColumnListDBEntry currColumn = colSchema.get(i);
				
				if(currColumn.columnType.type != currValue.valueType) {
					if(currValue.valueType == DBType.DBTypeSpecifier.DB_NULL) {
						if(currColumn.nullable == false)
							throw new InsertColumnNonNullableError(currColumn.columnName);
					}
					else
						throw new InsertTypeMismatchError();
				}
				
				// Char type truncate
				if(currColumn.columnType.type == DBType.DBTypeSpecifier.DB_CHAR) {
					currValue.trimChar(currColumn.columnType.length);
				}
			}
			
			// Validation 2. Duplicated Priamry Key
			ArrayList<DBValue> recordPK = new ArrayList<DBValue>();
			for(int i = 0; i < colSchema.size(); i++) {
				ColumnListDBEntry currColumn = colSchema.get(i);
				if(currColumn.primaryKey) 
					recordPK.add(record.get(i));
			}
			Database targetDB = myDBEnv.openDatabase(null, tableName, _dbOpenOrCreateCfg);
			if(recordPK.size() > 0) {
				DatabaseEntry pkKey = new DatabaseEntry(MyInterpreter.toBytes(recordPK));
				DatabaseEntry foundRecord = new DatabaseEntry();
				if(targetDB.get(null, pkKey, foundRecord, LockMode.DEFAULT) != OperationStatus.NOTFOUND) {
					targetDB.close();
					throw new InsertDuplicatePrimaryKeyError();
				}
			}
			
			// Validation 3. Referential Integrity
			ArrayList<ForeignKeyListDBEntry> fkSchema = getFKSchema(tableName);
			for(ForeignKeyListDBEntry currFK : fkSchema) {
				ArrayList<ColumnListDBEntry> refedTableColSchema = getColumnSchema(currFK.referencedTableName);
				
				// Extract foreign key from current record (in the order that appers in referenced table)
				ArrayList<DBValue> currRecordFK = new ArrayList<DBValue>();
				for(ColumnListDBEntry currCol : refedTableColSchema) {
					int fkIdx = currFK.referencedColList.indexOf(currCol.columnName);
					if(fkIdx != -1) {
						int recordColIdx = colSchemaMap.get(currFK.referencingColList.get(fkIdx)).columnIndex;
						currRecordFK.add(record.get(recordColIdx));
					}
				}
				
				// Checks if this FK of current record has a null value (-> always met referential integrity for this FK)
				boolean hasNull = false;
				for(DBValue v : currRecordFK) 
					if(v.isNull()) {
						hasNull = true;
						break;
					}
				if(hasNull) continue; // Move to the next FK
				
				// Checks if this FK of current record is in the referenced table
				Database refedTable = myDBEnv.openDatabase(null, currFK.referencedTableName, _dbOpenOrCreateCfg);
				DatabaseEntry fkKey = new DatabaseEntry(MyInterpreter.recordToBDBString(currRecordFK).getBytes("UTF-8"));
				DatabaseEntry foundRecord = new DatabaseEntry();
				
				if(refedTable.get(null, fkKey, foundRecord, LockMode.DEFAULT) == OperationStatus.NOTFOUND) {
					refedTable.close();
					throw new InsertReferentialIntegrityError();
				}
				
				refedTable.close();
			}
			
			// ----- If code reaches here, then there is no problem to insert the record! ---------------------------------- 
			// Insert the record to the BDB
			DatabaseEntry recordKey;
			DatabaseEntry recordData = new DatabaseEntry(MyInterpreter.toBytes(record));
			
			if(recordPK.size() == 0) { // If this table has no primary key 
				recordKey = new DatabaseEntry(UUID.randomUUID().toString().getBytes("UTF-8")); // Use Random-Generated UUID for BDB Key
				while(targetDB.putNoOverwrite(null, recordKey, recordData) == OperationStatus.KEYEXIST ) {
					recordKey = new DatabaseEntry(UUID.randomUUID().toString().getBytes("UTF-8")); // If conflict occurs, then generate a new key until no conflict occurs. 
				}
			}
			else {
				recordKey = new DatabaseEntry(MyInterpreter.recordToBDBString(recordPK).getBytes("UTF-8")); // Use PK(converted to string) of the record for BDB Key
				if(targetDB.put(null, recordKey, recordData) != OperationStatus.SUCCESS) {
					targetDB.close();
					throw new RuntimeException("Insertion failed for unexpected reason!!");
				}
			}
			System.out.println("The row is inserted");
			targetDB.close();
		}
		catch(DBError e)
		{
			throw e;
		}
		catch(UnsupportedEncodingException e)
		{
			e.printStackTrace();
		}
		finally
		{
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
	
	// Additional public methods for INSERT / DELETE / SELECT
	public ArrayList<ColumnListDBEntry> getColumnSchema(String tableName) {
		Database colSchemaDB = myDBEnv.openDatabase(null, "SCHEMA_COLUMN_" + tableName, _dbOpenOnlyCfg);
		Cursor colCursor = colSchemaDB.openCursor(null, null);
		ArrayList<ColumnListDBEntry> colSchemaList = new ArrayList<ColumnListDBEntry>();
		
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		colCursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
		do {
			ColumnListDBEntry colSchema = (ColumnListDBEntry)fromBytes(foundData.getData());
			colSchemaList.add(colSchema.columnIndex, colSchema);
		} while(colCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
		
		colCursor.close();
		colSchemaDB.close();
		
		return colSchemaList;
	}
	
	public ArrayList<ForeignKeyListDBEntry> getFKSchema(String tableName) {
		Database fkSchemaDB = myDBEnv.openDatabase(null, "SCHEMA_FOREIGNKEY_" + tableName, _dbOpenOnlyCfg);
		Cursor fkCursor = fkSchemaDB.openCursor(null, null);
		ArrayList<ForeignKeyListDBEntry> fkSchemaList = new ArrayList<ForeignKeyListDBEntry>();
		
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		if(fkSchemaDB.count() > 0) {
			fkCursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
			do {
				ForeignKeyListDBEntry fkSchema = (ForeignKeyListDBEntry)fromBytes(foundData.getData());
				fkSchemaList.add(fkSchema);
			} while(fkCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
		}
		
		return fkSchemaList;
	}
	
	public static String recordToBDBString(ArrayList<DBValue> record) {
		String result = "";
		for(DBValue v : record) {
			if(v.valueType == DBType.DBTypeSpecifier.DB_CHAR)
				result +=  ("\"" + v.toString() + "\"");
			else
				result += v.toString();
			
			result += "\'"; // delimeter for each columns
		}
		
		if(result.length() > 0)
			result.substring(0, result.length()-1); // deletes last delemeter
		
		return result;
	}
	
	public static ArrayList<DBValue> recordFromBDBString(String strBDB){
		String[] strFields = strBDB.split("\'");
		ArrayList<DBValue> record = new ArrayList<DBValue>(); 
		for(String s : strFields) {
			if(s.startsWith("\"")) // CHAR(n)
				record.add(new DBValue(s.substring(1, s.length()-1)));
			else if(s.matches("^([0-9]{4})-([0-9]{2})-([0-9]{2})$")) { // DATE
				String[] ymd = s.split("-");
				record.add(new DBValue(Integer.parseInt(ymd[0]), Integer.parseInt(ymd[1]), Integer.parseInt(ymd[2])));
			}
			else if(s.equals("NULL")) // NULL
				record.add(new DBValue());
			else // INT
				record.add(new DBValue(Integer.parseInt(s)));	
		}
		
		return record;
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
