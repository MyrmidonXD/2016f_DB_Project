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

public class JoinRecordIterator {
	
	private ArrayList<Database> _dbList;
	private ArrayList<Cursor> _cursorList;
	private ArrayList<Integer> _tableSizeList;
	private int[] _currTableIdxList;
	private boolean _isEnd;
	private boolean _isInitialState;

	public JoinRecordIterator(ArrayList<String> tableNameList) { // tableNameList must be validated first (with FromClause).
		_isEnd = false;
		_isInitialState = true;
		
		_currTableIdxList = new int[tableNameList.size()];
		
		MyInterpreter interpreter = MyInterpreter.getInstance();
		for(int i = 0; i < tableNameList.size(); i++) {
			String tblName = tableNameList.get(i);
			Database currDB = interpreter.myDBEnv.openDatabase(null, tblName, MyInterpreter._dbOpenOrCreateCfg);
			Cursor currCursor = currDB.openCursor(null, null);
			
			int currDBSize = (int)currDB.count();
			_tableSizeList.add(currDBSize);
			if(currDBSize == 0) _isEnd = true; // if a table has no record, then cartesian product produces no record.
			
			currCursor.getFirst(new DatabaseEntry(), new DatabaseEntry(), LockMode.DEFAULT);
			
			_dbList.add(currDB);
			_cursorList.add(currCursor);
			_currTableIdxList[i] = 0; 
		}
		
		_currTableIdxList[tableNameList.size() - 1] = -1; // modify last index to -1 
	}
	
	public void reInit() { // Note. this method must not be called after close() is called.
		_isEnd = false;
		_isInitialState = true;
		
		for(int i = 0; i < _cursorList.size(); i++) {
			_cursorList.get(i).getFirst(new DatabaseEntry(), new DatabaseEntry(), LockMode.DEFAULT);
			_currTableIdxList[i] = 0;
		}
		
		_currTableIdxList[_cursorList.size() - 1] = -1; // modify last index to -1 
	}
	
	public void close() {
		for(int i = 0; i < _dbList.size(); i++) {
			_cursorList.get(i).close();
			_dbList.get(i).close();
		}
		
	}
	
	public boolean hasNext() {
		return !_isEnd;
	}
	
	public ArrayList<DBValue> getNext() {
		int lastIdx = _cursorList.size() - 1;
		_currTableIdxList[lastIdx] += 1;
		
		if(!_isInitialState){ // Move cursors to the next combination.
			for(int i = lastIdx; i >= 0; i--) {
				if(_currTableIdxList[i] < _tableSizeList.get(i)) {
					_cursorList.get(i).getNext(new DatabaseEntry(), new DatabaseEntry(), LockMode.DEFAULT);
					break;
				}
				else {
					if(i > 0) {
						_currTableIdxList[i] = 0;
						_currTableIdxList[i-1] += 1;
						_cursorList.get(i).getFirst(new DatabaseEntry(), new DatabaseEntry(), LockMode.DEFAULT);
					}
				}
			}
			
			if(_currTableIdxList[0] >= _tableSizeList.get(0))
				_isEnd = true;
		}
		else
			_isInitialState = false;
		
		ArrayList<DBValue> resultRecord = new ArrayList<DBValue>();
		for(int i = 0; i < _cursorList.size(); i++) {
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			
			if(_cursorList.get(i).getCurrent(foundKey, foundData, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
				throw new RuntimeException("Failed to get current record in JoinRecordIterator!!");
			}
			
			ArrayList<DBValue> partialRecord = (ArrayList<DBValue>)MyInterpreter.fromBytes(foundData.getData());
			
			resultRecord.addAll(partialRecord);
		}
		
		return resultRecord;
	}
	
	public void removeCurrent() { // Only used in DELETE, for the case _dbList.size() == 1
		if(_dbList.size() != 1) return;
		
		if(_cursorList.get(0).delete() != OperationStatus.SUCCESS) {
			throw new RuntimeException("Failed to delete current record in JoinRecordIterator!!");
		}
		
		_currTableIdxList[0]--;
		
		int tblSize = _tableSizeList.get(0);
		_tableSizeList.remove(0);
		_tableSizeList.add(tblSize-1);
		
		_cursorList.get(0).getPrev(new DatabaseEntry(), new DatabaseEntry(), LockMode.DEFAULT); // TODO What if the first record has been removed?	
	}
}
