package Main;

public class Column {
	
	private String columnName;
	private String dataType;
	private byte code;
	private String isPrimaryKey;
	private String isNotNullable;
	
	
	public Column() {
		super();
		// TODO Auto-generated constructor stub
	}


	public Column(String columnName, String dataType, byte code, String isPrimaryKey, String isNotNullable) {
		super();
		this.columnName = columnName;
		this.dataType = dataType;
		this.code = code;
		this.isPrimaryKey = isPrimaryKey;
		this.isNotNullable = isNotNullable;
	}


	public String getColumnName() {
		return columnName;
	}


	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}


	public String getDataType() {
		return dataType;
	}


	public void setDataType(String dataType) {
		this.dataType = dataType;
	}


	public String isPrimaryKey() {
		return isPrimaryKey;
	}


	public void setPrimaryKey(String isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}


	public String isNotNullable() {
		return isNotNullable;
	}


	public void setNotNullable(String isNotNullable) {
		this.isNotNullable = isNotNullable;
	}


	public byte getCode() {
		return code;
	}


	public void setCode(byte code) {
		this.code = code;
	}


		

}
