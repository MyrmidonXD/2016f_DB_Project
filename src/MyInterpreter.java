// imports
/* BerkelyDB classes */
import java.io.File;
import java.util.ArrayList;

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

	private MyInterpreter() {
	    // Open Database Environment or if not exists, create one.
	    EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    myDBEnv = new Environment(new File("db/"), envConfig);
	    
	    // Open Table List Database or if not exists, create one.
	    tableListDB = myDBEnv.openDatabase(null, "SCHEMA_TableList", _dbOpenOrCreateCfg);
	}
	 
	public static MyInterpreter getInstance() {
		if(_instance == null)
			_instance = new MyInterpreter();
		return _instance;
	}
	
	public boolean createTable() {
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
	
}
