package Main;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;

import Exception.InvalidCommandException;
import common.Constants;
import common.ResultSet;
import common.TBLFiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static java.lang.System.out;
public class DavisBasePrompt {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "©2016 Akash Chand";
	static boolean isExit = false;
	static int tableInsertLocation = 0;
	static int columnInsertLocation = 0;
	/*
	 * Page size for alll files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	public static long pageSize = 512; 

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();
		initialize();
		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	private static void initialize() {
		// TODO Auto-generated method stub
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		String columnCatalog = Constants.catalogDataPath+"columns.tbl";
		File fileTableCatalog = new File(tableCatalog);
		File fileColumnCatalog = new File(columnCatalog);
		if(!fileColumnCatalog.exists()) {
			System.out.println("here");
			try {
				/*  Create RandomAccessFile tableFile in read-write mode.
				 *  Note that this doesn't create the table file in the correct directory structure
				 */
				RandomAccessFile tableFile = new RandomAccessFile(tableCatalog, "rw");
				//tableFile.setLength(pageSize);
				tableFile.seek(fileTableCatalog.length());
				tableFile.writeByte(2);// Initial number of records
				//tableFile.writeShort(512); // initial pointer location i.e. end of file
	//			tableFile.writeInt(-1); // address of next leaf -->  decimal(-1) = hex(FF FF FF FF)
	//			tableFile.write
				tableFile.writeByte(0);//deletebit
				tableFile.writeByte(1);//rowid
				tableFile.writeUTF("tables");
				tableFile.writeInt(-1);
				tableFile.writeByte(0);//deletebit
				tableFile.writeByte(2);
				tableFile.writeUTF("columns");
				tableFile.writeInt(-1);
				tableFile.close();
				
				RandomAccessFile colFile = new RandomAccessFile(columnCatalog, "rw");
				//tableFile.setLength(pageSize);
				colFile.seek(fileColumnCatalog.length());
				colFile.writeByte(7);// Initial number of records
				colFile.writeByte(0);//deletebit
				colFile.writeInt(1);//rowid
				colFile.writeUTF("tables");
				colFile.writeUTF("tables");
				colFile.writeByte(0x0c+6);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.writeByte(0);//deletebit
				colFile.writeInt(2);//rowid
				colFile.writeUTF("columns");
				colFile.writeUTF("rowid");
				colFile.writeByte(0x0c+5);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.writeByte(0);//deletebit
				colFile.writeInt(3);//rowid
				colFile.writeUTF("columns");
				colFile.writeUTF("tablename");
				colFile.writeByte(0x0c+9);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.writeByte(0);//deletebit
				colFile.writeInt(4);//rowid
				colFile.writeUTF("columns");
				colFile.writeUTF("columnname");
				colFile.writeByte(0x0c+10);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.writeByte(0);//deletebit
				colFile.writeInt(5);//rowid
				colFile.writeUTF("columns");
				colFile.writeUTF("datatype");
				colFile.writeByte(0x0c+8);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.writeByte(0);//deletebit
				colFile.writeInt(6);//rowid
				colFile.writeUTF("columns");
				colFile.writeUTF("primarykey");
				colFile.writeByte(0x0c+10);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.writeByte(0);//deletebit
				colFile.writeInt(7);//rowid
				colFile.writeUTF("columns");
				colFile.writeUTF("notnull");
				colFile.writeByte(0x0c+7);
				colFile.writeUTF("NULL");
				colFile.writeUTF("NO");
				colFile.close();
	
				
			}
			catch(Exception e) {
				System.out.println(e);
			}
		}
	}

	/** ***********************************************************************
	 *  Static method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			out.println(line("*",80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			//printCmd("SELECT * FROM <table_name>;");
			//printDef("Display all records in the table <table_name>.");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("\tis <column_name> = <value>.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println(line("*",80));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		

		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "show":
				showTables();
				break;
			case "select":
			try {
				parseQuery(userCommand);
			} catch (InvalidCommandException e1) {
				// TODO Auto-generated catch block
				e1.getMessage();
			}
				break;
			case "drop":
				//System.out.println("CASE: DROP");
				dropTable(userCommand);
				break;
			case "create":
				//System.out.println("CASE: CREATE");
				//parseCreateTable(userCommand);
				try {
					validateCreate(userCommand);
				} catch (InvalidCommandException e) {
					// TODO Auto-generated catch block
					e.getMessage();
				}
				break;
			case "insert":
				//System.out.println("CASE: INSERT");
				try {
					validateInsert(userCommand);
				} catch (InvalidCommandException e) {
					// TODO Auto-generated catch block
					System.out.println(e.getMessage());
				}
				break;
			case "update":
				//System.out.println("CASE: UPDATE");
				parseUpdate(userCommand);
				break;
			case "delete":
				parseDelete(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	

	private static void parseDelete(String userCommand) {
		// TODO Auto-generated method stub
		String tableName = userCommand.split(" ")[2].trim();
		ArrayList<ReadColumn> colDTypeList =null;
		String whereCol=null;
		String operator = null;
		String whereVal = null;
		colDTypeList = TBLFiles.readCatalogFile(tableName);
		if(userCommand.contains("where")) {
			String temp = userCommand.replaceAll("\''","null").replaceAll("\'","").trim();
			String wheresubstr = temp.substring(temp.indexOf("where")+5);
			String wh[] = wheresubstr.trim().split(" ");
			whereCol = wh[0].trim();
			operator = wh[1].trim();
			whereVal = wh[2].trim();
			for (ReadColumn col : colDTypeList) {
				if(col.getColumnName().equals(whereCol)) {
					col.setWhere(true);
				}
			}
		}
		for (ReadColumn col : colDTypeList) {
			col.setIsSelected(true);
		}
		ResultSet rs = TBLFiles.select(tableName , colDTypeList, whereCol , whereVal, operator);
		TBLFiles.deleteQuery(tableName, colDTypeList , rs);
	}

	private static void showTables() {
		// TODO Auto-generated method stub
		TBLFiles.showTable();
	}

	private static void validateInsert(String userCommand) throws InvalidCommandException {
		// TODO Auto-generated method stub
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		String columnInfo = userCommand.substring(userCommand.indexOf("(") + 1, userCommand.length() - 1);
		//System.out.println("Result: "+ columnInfo.replaceAll("\''","null"));
		String[] values = columnInfo.replaceAll("\''","null").replaceAll("\'","").trim().replaceAll("\\s+","").split(",");
		
		ArrayList<String> colData = new ArrayList<String>(Arrays.asList(values));
		//System.out.println(createTableTokens.get(2));
		ArrayList<ReadColumn> colDTypeList = TBLFiles.readCatalogFile(createTableTokens.get(2));
		int index=0; String pkCol="", pkColData = "", cd ="";
		for (ReadColumn rc:colDTypeList) {
			//ReadColumn rc = colDTypeList.get(index);
			
			if(colData.isEmpty()) {
				cd = "";
			}else {
				cd = colData.get(0);
			}
			if(rc.getIsPrimaryKey().equals("pri")) {
				pkCol=rc.getColumnName();
				pkColData= cd;
				rc.setIsSelected(true);
				rc.setWhere(true);
			}
			if(rc.getIsNotNullable().equals("yes")) {
				if (cd.equals("") || cd == null) {
					//System.out.println("NOT NULL COLUMN VALUE "+cd);
					throw new InvalidCommandException(rc.getColumnName()+ " field cannot be null!!");
				}
			}
			rc.setColData(cd);
			colData.remove(0);
		}
		ResultSet rs = TBLFiles.select(createTableTokens.get(2), colDTypeList, pkCol, pkColData, "=");
		index=0;
//		for(String x : rs.getResult()) {
//			System.out.println(x);
//			System.out.println(i++);
//		}
		if(!rs.getResult().isEmpty()) {
			throw new InvalidCommandException("Primary key value already exists!!");
		}
		colData = new ArrayList<String>(Arrays.asList(values));
		for (String c:colData) {
			ReadColumn rc = colDTypeList.get(index);
			rc.setColData(c);
			++index;
		}
		TBLFiles.writeFile(createTableTokens.get(2), colDTypeList);
		System.out.println("1 row inserted!!");
	}

	public static void validateCreate(String userCommand) throws InvalidCommandException {
		// TODO Auto-generated method stub
		
		String [] dataTypes = {"byte","short","int","bigint","real","double","datetime","date", "text"};
		ArrayList<String> dTypes = new ArrayList<>(Arrays.asList(dataTypes));
		String colAndDataTypes = userCommand.substring(userCommand.indexOf("(")+1,userCommand.lastIndexOf(")"));
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(colAndDataTypes.split(",")));
		ArrayList<Column> colList = new ArrayList<>();
		for (String token:createTableTokens) {
		//	System.out.println(token.trim().split(" ")[1].trim());
			if(!dTypes.contains(token.trim().split(" ")[1].trim().toLowerCase())) {
				throw new InvalidCommandException("ERROR: Invalid Datatype");
				
			}
			else {
				String [] colData = token.trim().split(" ");
				if(colData.length<2) {
					
					throw new InvalidCommandException("ERROR: Invalid syntax");
					
				}
				String colname = colData[0].trim().toLowerCase();
				
				String coldatatype = colData[1].trim().toLowerCase();
				//System.out.println("Col Name : "+colname + " col data type :"+coldatatype);
				byte colcode = 0;
				String colPK = "null";
				String colnotNull = "no";
				if(colData.length-2>0) {
					if(colData[2].trim().equals("primarykey")) {
						colPK = "pri";
						if(colData.length-3>0) {
							if(colData[3].trim().equals("notnull"))
								colnotNull = "yes";
						}
						
					}else {
						if(colData[2].trim().equals("notnull"))
							colnotNull = "yes";
					}
				}
				
				
			//	System.out.println("Pk : "+colPK+" notnull : "+colnotNull);
				if(coldatatype.equals("int")) {
					colcode=Constants.INTcode;
					//System.out.println(" int code :"+ colcode);
				}else if(coldatatype.equals("byte")) {
					colcode=Constants.tinyIntcode;
				}else if(coldatatype.equals("short")) {
					colcode=Constants.smallINTcode;
				}else if(coldatatype.equals("bigint")) {
					colcode=Constants.bigINTcode;
				}else if(coldatatype.equals("real")) {
					colcode=Constants.serialREALcode;
				}else if(coldatatype.equals("double")) {
					colcode=Constants.serialDbcode;
				}else if(coldatatype.equals("datetime")) {
					colcode=Constants.DATETIMEcode;
				}else if(coldatatype.equals("date")) {
					colcode=Constants.serialDATEcode;
				}else if(coldatatype.equals("text")) {
					//int l = Integer.parseInt(colname.substring(4)); 
					int l = colname.length();
					colcode=(byte) (Constants.textcode+l);
					//System.out.println(" text code :"+ colcode);
				}
				Column col = new Column(colname, coldatatype, colcode, colPK, colnotNull);
				colList.add(col);
			}
		}
		String tableName = userCommand.split(" ")[2].trim();
		parseCreateTable(colList, tableName);

		
	}

	public static void parseCreateTable(ArrayList<Column> colList, String tableName) {
		
		//System.out.println("Parsing the string:\"" + createTableString + "\"");
		//ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		/* Define table file name */
		//String tableFileName = createTableTokens.get(2) + ".tbl";

		/* YOUR CODE GOES HERE */
		
		/*  Code to create a .tbl file to contain table data */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 *  Note that this doesn't create the table file in the correct directory structure
			 */
			short addr = TBLFiles.checkTable(tableName);
			RandomAccessFile tableFile = new RandomAccessFile(Constants.userDataPath+tableName+".tbl", "rw");
			tableFile.setLength(pageSize);
			tableFile.seek(0);
			tableFile.writeByte(0x0D);
			tableFile.writeByte(0);// Initial number of records
			tableFile.writeShort((int) (pageSize)); // initial pointer location i.e. end of file
			tableFile.writeInt(-1); // address of next leaf -->  decimal(-1) = hex(FF FF FF FF)
			
			tableFile.close();
			TBLFiles.writeCatalog(tableName, colList);
			
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
		/*  Code to insert a row in the davisbase_tables table 
		 *  i.e. database catalog meta-data 
		 */
		
		/*  Code to insert rows in the davisbase_columns table  
		 *  for each column in the new table 
		 *  i.e. database catalog meta-data 
		 */
	}
	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		
		String[] tokens = dropTableString.split(" ");
		String tableName = tokens[2].trim();
		if (TBLFiles.dropTable(tableName))
		{
			System.out.println("Table dropped");
		}else {
			System.out.println("Error!! Failed to drop table");
		}
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 * @throws InvalidCommandException 
	 */
	public static void parseQuery(String queryString) throws InvalidCommandException {
		
		String selectCols = queryString.substring(6, queryString.indexOf("from"));
		selectCols = selectCols.trim();
		String tableName =""; String whereCol = null, whereValue = null;String operator =null;
		ArrayList<ReadColumn> colDTypeList =null;
		if (queryString.contains("where")) {
			tableName = queryString.substring(queryString.indexOf("from")+4 , queryString.indexOf("where")).trim();
			String wheresubstr[] =queryString.replaceAll("\''","null").replaceAll("\'","").trim().substring(queryString.indexOf("where")+5).trim().split(" ");
			whereCol = wheresubstr[0].trim();
			whereValue = wheresubstr[2].trim();
			operator = wheresubstr[1].trim();
		}else
			tableName = queryString.substring(queryString.indexOf("from")+4 ).trim();
		boolean flag = TBLFiles.isTableDel(tableName);
		if(flag) {
			throw new InvalidCommandException("Error!! Cannot find table");
		}
		else {
			if(selectCols.equals("*")) {
				//CALL SELECT ALL METHOD HERE
				
				selectCols ="";
				colDTypeList = TBLFiles.readCatalogFile(tableName);
				for (ReadColumn col : colDTypeList) {
					col.setIsSelected(true);
					selectCols = selectCols +" , "+ col.getColumnName();
				}
				selectCols = selectCols.trim().substring(1).trim();
				//System.out.println(selectCols);
				if (queryString.contains("where")) {
					for (ReadColumn col : colDTypeList) {
						if(col.getColumnName().equals(whereCol)) {
							col.setWhere(true);
						}
					}
				}
	//			ResultSet rs = TBLFiles.select(tableName , colDTypeList, whereCol , whereValue,operator);
	//			rs.displayResult(selectCols,colDTypeList);
				}else {
				ArrayList <String>colList = new ArrayList<>(Arrays.asList(selectCols.split(",")));
				colDTypeList = TBLFiles.readCatalogFile(tableName);
				int index=1;
				for(String x : colList) {
					for (ReadColumn col : colDTypeList) {
						if(col.getColumnName().equals(x.trim())) {
							col.setIsSelected(true);
							col.setSelectIndex(index++);
						}
					}
				}
				if (queryString.contains("where")) {
					for (ReadColumn col : colDTypeList) {
						if(col.getColumnName().equals(whereCol)) {
							col.setWhere(true);
						}
					}
				}
				}
			ResultSet rs = null;
			if(whereCol!=null && whereCol.equals("rowid")) {
				rs = TBLFiles.selectRowId(tableName , colDTypeList, whereCol , whereValue, operator);
			}else {
				rs = TBLFiles.select(tableName , colDTypeList, whereCol , whereValue, operator);
			}
			rs.displayResult(selectCols,colDTypeList);
		
		}
	}

	/**
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		
		String tableName = updateString.split(" ")[1].trim();
		ArrayList<ReadColumn> colDTypeList =null;
		String whereCol=null;
		String operator = null;
		String whereVal = null;
		String updateVal=null;
		String updatecol = updateString.substring(updateString.indexOf("set")+3, updateString.indexOf("=")).trim();
		colDTypeList = TBLFiles.readCatalogFile(tableName);
		if(updateString.contains("where")) {
			String temp = updateString.replaceAll("\''","null").replaceAll("\'","").trim();
			updateVal = temp.substring(temp.indexOf("=")+1, temp.indexOf("where")).trim();
			String wheresubstr = temp.substring(temp.indexOf("where")+5);
			String wh[] = wheresubstr.trim().split(" ");
			whereCol = wh[0].trim();
			operator = wh[1].trim();
			whereVal = wh[2].trim();
			//colDTypeList = TBLFiles.readCatalogFile(tableName);
			for (ReadColumn col : colDTypeList) {
				if(col.getColumnName().equals(whereCol)) {
					col.setWhere(true);
				}
			}
		}else {
			System.out.println(updateString.replaceAll("\''","null").replaceAll("\'","").trim());
			String temp = updateString.replaceAll("\''","null").replaceAll("\'","").trim();
			updateVal = temp.substring(temp.indexOf("=")+1).trim();
		}
		
		for (ReadColumn col : colDTypeList) {
			if(col.getColumnName().equals(updatecol.trim())) {
				col.setIsSelected(true);
			}
		}
		ResultSet rs = TBLFiles.select(tableName , colDTypeList, whereCol , whereVal, operator);
		//rs.displayResult(updatecol,colDTypeList);
		TBLFiles.update(tableName, colDTypeList , updateVal, rs);
	}

	
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 */


	
}
