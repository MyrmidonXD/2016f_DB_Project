public class DBType {
	
	public enum DBTypeSpecifier { DB_INT, DB_CHAR, DB_DATE };
	
	public DBTypeSpecifier type;
	public int length; // only used for DB_CHAR
	

	public DBType(String typeName, int value) {
		if(typeName.equals("char"))
		{
			if(value > 0)
			{
				type = DBTypeSpecifier.DB_CHAR;
				length = value;
			}
			// else throw CharLengthError;			
		}
		// else throw some arbitrary exception which implies wrong constructor call.
	}
	
	public DBType(String typeName)
	{
		if(typeName.equals("int")) 
			type = DBTypeSpecifier.DB_INT;
		else if(typeName.equals("date")) 
			type = DBTypeSpecifier.DB_DATE;
		// else throw some arbitrary exception which implies wrong constructor call. 
		
		length = -1; // Not used
	}
	
	@Override
	public String toString(){
		switch(type)
		{
			case DB_INT:
				return "int";
			case DB_DATE:
				return "date";
			case DB_CHAR:
				return "char(" + Integer.toString(length) + ")";
			default:
				return "TypeError";
		}
	}
}
