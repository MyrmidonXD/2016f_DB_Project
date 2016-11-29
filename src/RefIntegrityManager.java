import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.ArrayList;

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

public class RefIntegrityManager {
	
	private String _myTableName;
	private ArrayList<Integer> _myPKIdx;
	private ArrayList<String> _myPKColName;
	private ArrayList<ArrayList<SimpleFKInfo>> _fkInfoListPerTable;


	public RefIntegrityManager(String tableName) {
		MyInterpreter interpreter = MyInterpreter.getInstance();
		ArrayList<ColumnListDBEntry> colSchemaList = interpreter.getColumnSchema(tableName);		
		
		_myTableName = tableName;
		_myPKIdx = new ArrayList<Integer>();
		_myPKColName = new ArrayList<String>();
		for(ColumnListDBEntry colSchema: colSchemaList) {
			if(colSchema.primaryKey) {
				_myPKIdx.add(colSchema.columnIndex);
				_myPKColName.add(colSchema.columnName);
			}
		}
		
		HashMap<String, ArrayList<SimpleFKInfo>> tableFKMap = new HashMap<String, ArrayList<SimpleFKInfo>>();
		ArrayList<String> tblNameList = interpreter.getTableList();
		for(String tblName : tblNameList) {
			if(tblName == _myTableName) continue;
			ArrayList<ColumnListDBEntry> tblColSchemaList = interpreter.getColumnSchema(tblName);
			ArrayList<ForeignKeyListDBEntry> tblFKSchemaList = interpreter.getFKSchema(tblName);
			HashMap<String, Integer> tblColIdxMap = new HashMap<String, Integer>();
			for(ColumnListDBEntry tblCol : tblColSchemaList) {
				tblColIdxMap.put(tblCol.columnName, tblCol.columnIndex);
			}
			
			for(ForeignKeyListDBEntry tblFK : tblFKSchemaList) {
				if(!tblFK.referencedTableName.equals(_myTableName)) continue; // Ignore FKs that doesn't reference myTable ('tableName')
				
				SimpleFKInfo fkInfo = new SimpleFKInfo(tblName);
				
				HashMap<String, Integer> myColTblIdxMap = new HashMap<String, Integer>();
				for(int i = 0; i < tblFK.referencingColList.size(); i++) {
					String myColName = tblFK.referencedColList.get(i);
					String tblColName = tblFK.referencingColList.get(i);
					myColTblIdxMap.put(myColName, tblColIdxMap.get(tblColName));
				}
				
				boolean isTblFKNullable = true;
				for(String myColName : _myPKColName) {
					int matchingTblIdx = myColTblIdxMap.get(myColName);
					fkInfo.orderedFKIdx.add(matchingTblIdx);
					if(tblColSchemaList.get(matchingTblIdx).nullable == false)
						isTblFKNullable = false; // if a colunm in fk is not nullable, then its SimpleFKInfo is not nullable.
				}
				
				fkInfo.isNullable = isTblFKNullable;
				
				if(!tableFKMap.containsKey(tblName)) {
					ArrayList<SimpleFKInfo> fkInfoList = new ArrayList<SimpleFKInfo>();
					fkInfoList.add(fkInfo);
					tableFKMap.put(tblName, fkInfoList);
				}
				else {
					ArrayList<SimpleFKInfo> fkInfoList = tableFKMap.get(tblName);
					fkInfoList.add(fkInfo);
				}
			}
		}
		
		_fkInfoListPerTable = new ArrayList<ArrayList<SimpleFKInfo>>(tableFKMap.values());
	}
	
	public boolean checkDeletable(ArrayList<DBValue> record) {
		boolean deletable = true;
		MyInterpreter interpreter = MyInterpreter.getInstance();
		ArrayList<DBValue> recordPK = new ArrayList<DBValue>();
		for(int i : _myPKIdx) {
			recordPK.add(record.get(i));
		}
		
		for(ArrayList<SimpleFKInfo> fkInfoList : _fkInfoListPerTable) {
			if(!deletable) break;
			String fromTable = fkInfoList.get(0).fromTable;
			
			Database fromTableDB = interpreter.myDBEnv.openDatabase(null, fromTable, MyInterpreter._dbOpenOrCreateCfg);
			Cursor fromTableCursor = fromTableDB.openCursor(null, null);
			
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			
			if(fromTableDB.count() > 0) {
				fromTableCursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
				do {
					ArrayList<DBValue> fromRecord = (ArrayList<DBValue>)MyInterpreter.fromBytes(foundData.getData());
					for(SimpleFKInfo fkInfo : fkInfoList) {
						if(!deletable) break;
						
						ArrayList<DBValue> fromFK = new ArrayList<DBValue>();
						for(int i : fkInfo.orderedFKIdx)
							fromFK.add(fromRecord.get(i));
						
						if(recordPK.equals(fromFK) && !fkInfo.isNullable)
							deletable = false; // fromRecord points record, and its fk is not nullable => the record is not deletable.
					}
					
					if(!deletable) break;
				} while(fromTableCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
			}
			
			fromTableCursor.close();
			fromTableDB.close();
		}
		
		return deletable;
	}
	
	public void cascadeDelete(ArrayList<DBValue> record) { // Note. checkDeletable must be called and checked (about the record) prior to this method.
		MyInterpreter interpreter = MyInterpreter.getInstance();
		ArrayList<DBValue> recordPK = new ArrayList<DBValue>();
		for(int i : _myPKIdx) {
			recordPK.add(record.get(i));
		}
		
		for(ArrayList<SimpleFKInfo> fkInfoList : _fkInfoListPerTable) {
			String fromTable = fkInfoList.get(0).fromTable;
			
			Database fromTableDB = interpreter.myDBEnv.openDatabase(null, fromTable, MyInterpreter._dbOpenOrCreateCfg);
			Cursor fromTableCursor = fromTableDB.openCursor(null, null);
			
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			
			if(fromTableDB.count() > 0) {
				fromTableCursor.getFirst(foundKey, foundData, LockMode.DEFAULT);
				do {
					ArrayList<DBValue> fromRecord = (ArrayList<DBValue>)MyInterpreter.fromBytes(foundData.getData());
					boolean fromRecordModified = false;
					for(SimpleFKInfo fkInfo : fkInfoList) {
						
						ArrayList<DBValue> fromFK = new ArrayList<DBValue>();
						for(int i : fkInfo.orderedFKIdx)
							fromFK.add(fromRecord.get(i));
						
						if(recordPK.equals(fromFK)) {
							for(int i : fkInfo.orderedFKIdx) { // replace fromRecord[i] with null.
								fromRecord.set(i, new DBValue());
							}
							fromRecordModified = true;
						}
					}
					if(fromRecordModified) {
						DatabaseEntry modifiedFromRecord = new DatabaseEntry(MyInterpreter.toBytes(fromRecord));
						if(fromTableCursor.putCurrent(modifiedFromRecord) != OperationStatus.SUCCESS) {
							throw new RuntimeException("putCurrent failed in cascadeDelete()");
						}
					}
					
				} while(fromTableCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
			}
			
			fromTableCursor.close();
			fromTableDB.close();
		}
	}
}

class SimpleFKInfo {
	public ArrayList<Integer> orderedFKIdx;
	public String fromTable;
	public boolean isNullable;
	
	public SimpleFKInfo(String tblFromName) {
		fromTable = tblFromName;
		orderedFKIdx = new ArrayList<Integer>();
		boolean isNullable = true;
	}
	
}
