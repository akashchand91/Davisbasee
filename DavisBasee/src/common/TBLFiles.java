package common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;

import Exception.InvalidCommandException;
import Main.Column;
import Main.DavisBasePrompt;
import Main.ReadColumn;

public class TBLFiles {
	

	static byte startLocation=0;
	
	public static short getRecordLength(ArrayList<ReadColumn> colDTypeList) {
		
		short recordLength = 5; //  1 for delete bit and 4 for rowid
		for(ReadColumn dtype:colDTypeList) {
			//System.out.println("datatype :"+dtype.getDataType());
			if(dtype.getDataType().equals("int")) {
				recordLength = (short) (recordLength+Constants.INTSIZE);
			}else if(dtype.getDataType().equals("byte")) {
				recordLength = (short) (recordLength+Constants.TINYINTSIZE);
			}else if(dtype.getDataType().equals("short")) {
				recordLength = (short) (recordLength+Constants.SMALLINTSIZE);
			}else if(dtype.getDataType().equals("bigint")) {
				recordLength = (short) (recordLength+Constants.BIGINTSIZE);
			}else if(dtype.getDataType().equals("real")) {
				recordLength = (short) (recordLength+Constants.REALSIZE);
			}else if(dtype.getDataType().equals("double")) {
				recordLength = (short) (recordLength+Constants.DOUBLESIZE);
			}else if(dtype.getDataType().equals("datetime")) {
				//recordLength = (short) (recordLength+Constants.DATETIMESIZE);
				int l = dtype.getColData().length(); 
				//System.out.println("Text length :"+l);
				recordLength = (short) (recordLength+2+l);
			}else if(dtype.getDataType().equals("date")) {
				//recordLength = (short) (recordLength+Constants.DATESIZE);
				int l = dtype.getColData().length(); 
				//System.out.println("Text length :"+l);
				recordLength = (short) (recordLength+2+l);
			}else if(dtype.getDataType().substring(0, 4).equals("text")) {
				int l = dtype.getColData().length(); 
				//System.out.println("Text length :"+l);
				recordLength = (short) (recordLength+2+l);
			}
		}
		return recordLength;
	}
	/*
	 * Generic function to write to user .tbl files. Parameters : Table name and column data
	 * */
	public static boolean writeFile(String tableName, ArrayList<ReadColumn> columns) {

		int rootPage = getRootPage(tableName);
		//System.out.println("Root page "+rootPage);
		
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		File tabFile = new File(tabFileName);
		if(tabFile.exists()) {
			if(rootPage == -1) {
				writeToPage(tabFile, tabFileName, columns ,startLocation , rootPage ,(byte) 0);
			}else {
				int writePageAddress = getWritePageAddress(tabFile,rootPage);
				if(writePageAddress == -1) {
					System.out.println("UNKNOWN ERROR occured while fetching write page address");
				}else {
					int numRec = getNumRec(tabFile, rootPage);
					writeToPage(tabFile, tabFileName, columns ,writePageAddress , rootPage ,(byte) numRec);
				}
			}
		}else {
			return false;
		}
		
		return true;
	}

	private static int getNumRec(File tabFile, int rootPage) {
		// TODO Auto-generated method stub
		
		try {
			RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
			tabRAF.seek(rootPage);
			tabRAF.readByte();
			int numChild = tabRAF.readByte(); // Number of left children
			int pageRecLength = 5;
			int backStep = ((numChild)*pageRecLength);
			tabRAF.seek(rootPage+ DavisBasePrompt.pageSize-backStep);
			int numRec = tabRAF.readByte();
			tabRAF.close();
			return numRec;
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	/*
	 * Gets the address of the right most leaf page 
	 */
	public static int getWritePageAddress(File tabFile, int rootPage) {
		// TODO Auto-generated method stub
		//String tableFileAddress = Constants.userDataPath+tabFile+".tbl";
		try {
			RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
			tabRAF.seek(rootPage+2);
			int address = tabRAF.readInt();
			
			tabRAF.close();
			return address;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	public static void writeToPage(File tabFile, String tabFileName, ArrayList<ReadColumn> columns, int startLocation2, int rootPage ,  byte numRec) {
		// TODO Auto-generated method stub
//		for(ReadColumn rc : columns) {
//			System.out.println("data : "+rc.getColData()+" datatype :  "+rc.getDataType()+" column name : "+rc.getColumnName()+" code : "+rc.getCode());
//		}
		//TBLFiles tblobj = new TBLFiles();
		//ArrayList<ReadColumn> colDTypeList = readCatalogFile(tabFileName);
		short recordLength = (short) (getRecordLength(columns));
		//System.out.println("recordLength : "+recordLength);
		try {
			RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
			tabRAF.seek(startLocation2);
			tabRAF.readByte(); // leaf page 0x0D
			if(rootPage==-1)
				numRec = tabRAF.readByte();
			else
				numRec += tabRAF.readByte();
			short offset = tabRAF.readShort();
			int nextPage = tabRAF.readInt();
			short lastpos = (short) (startLocation2+8+numRec*2);
			short diff = (short) (offset-lastpos);
			//short diff = 5;
			//System.out.println(" Offset : "+offset+" difference : "+diff);
			if (diff<recordLength) { //CODE FOR B+TREE AND NEW PAGE CREATION GOES HERE
				if(rootPage == -1) {
					createRoot(tabFileName, columns, numRec,startLocation2+DavisBasePrompt.pageSize, startLocation2);
				}
				else
					writetoRootPage(tabFileName, columns, rootPage, numRec , startLocation2+DavisBasePrompt.pageSize, startLocation2);
				tabRAF.close();
			}
			else {
				int newOffset = offset-recordLength;
				tabRAF.seek(newOffset);
				tabRAF.writeByte(0); //delete bit
				tabRAF.writeInt(++numRec); //row id
				for(ReadColumn dtype:columns) {
					if(dtype.getDataType().equals("int")) {
						tabRAF.writeInt(Integer.parseInt(dtype.getColData()));
					}else if(dtype.getDataType().equals("byte")) {
						tabRAF.writeByte(Integer.parseInt(dtype.getColData()));
					}else if(dtype.getDataType().equals("short")) {
						tabRAF.writeShort(Integer.parseInt(dtype.getColData()));
					}else if(dtype.getDataType().equals("bigint")) {
						tabRAF.writeLong(Long.parseLong(dtype.getColData()));
					}else if(dtype.getDataType().equals("real")) {
						tabRAF.writeFloat(Float.parseFloat(dtype.getColData()));
					}else if(dtype.getDataType().equals("double")) {
						tabRAF.writeDouble(Double.parseDouble(dtype.getColData()));
					}else if(dtype.getDataType().equals("datetime")) {
						tabRAF.writeUTF(dtype.getColData());
					}else if(dtype.getDataType().equals("date")) {
						tabRAF.writeUTF(dtype.getColData());
					}else if(dtype.getDataType().equals("text")) {
						tabRAF.writeUTF(dtype.getColData());
					}
				}
				tabRAF.seek(startLocation2);
				tabRAF.readByte();
				int numR = tabRAF.readByte();
				tabRAF.seek(startLocation2);
				tabRAF.readByte();
				tabRAF.writeByte(++numR);
				short recpos = (short) newOffset;
				tabRAF.writeShort(recpos);
				lastpos = (short) (startLocation2+8+(numR-1)*2); //writing references to the individual record
				tabRAF.seek(lastpos);
				tabRAF.writeShort(recpos);
				tabRAF.close();
			}
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void writetoRootPage(String tabFileName, ArrayList<ReadColumn> columns, int rootPage, byte numRec, long l, int startLocation2) {
		// TODO Auto-generated method stub
		try {
			RandomAccessFile tabRAF = new RandomAccessFile(tabFileName, "rw");
			tabRAF.seek(rootPage);
			tabRAF.readByte();
			int numChild = tabRAF.readByte(); // Number of left children
			int newLeftleafAddress = tabRAF.readInt();
			tabRAF.seek(rootPage);
			tabRAF.readByte();
			tabRAF.writeByte(numChild+1);
			tabRAF.writeInt((int) l); // new address of the right most leaf
			
			int pageRecLength = 5;
			tabRAF.seek(rootPage+DavisBasePrompt.pageSize-((numChild+1)*pageRecLength));
			tabRAF.writeByte(numRec+1);
			tabRAF.writeInt(newLeftleafAddress);//address of the new left leaf
			
			tabRAF.seek(startLocation2+4);//writing address of the next leaf page on the previous leaf page
			tabRAF.writeInt((int) l);
			
			tabRAF.seek(l);
			tabRAF.setLength(l+DavisBasePrompt.pageSize); //number of left child + root + new page 
			tabRAF.writeByte(0x0D); //leaf page
			tabRAF.writeByte(0); // number of records in the new page- which is initially 0
			tabRAF.writeShort((int) (l+DavisBasePrompt.pageSize)); // end of the page
			tabRAF.writeInt(-1); //next page location
			tabRAF.close();
			File tabFile = new File (tabFileName);
			writeToPage(tabFile, tabFileName, columns, (int) (l), rootPage, numRec);
			
//			for(int i=0;i<numChild;i++) {
//				tabRAF.seek(rootPage+DavisBasePrompt.pageSize-pageRecLength);
//				int rowID = tabRAF.readByte(); // Getting the max rowid
//				tabRAF.readInt();
//				pageRecLength+=5;
//			}
//			int newRowId = 
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	private static void createRoot(String tabFileName, ArrayList<ReadColumn> columns, byte numRec, long firstPos, int startLocation2) {
		// TODO Auto-generated method stub
	
		try {
			RandomAccessFile tabRAF = new RandomAccessFile(tabFileName, "rw");
			tabRAF.seek(firstPos);
			tabRAF.writeByte(0x05); // Interior page
			tabRAF.writeByte(1); // Only one children to the left
			tabRAF.writeInt((int) (firstPos+DavisBasePrompt.pageSize)); // address of the right most node
			tabRAF.seek(firstPos+DavisBasePrompt.pageSize-5);// 4 bytes address of the left node and 1 byte is max row id
			tabRAF.writeByte(numRec); // row id
			tabRAF.writeInt(0); // address of the page
			updateCatTab(tabFileName,firstPos);
			File tabFile = new File (tabFileName);
			tabRAF.seek(firstPos);
			tabRAF.readByte();
			int numChild = tabRAF.readByte(); // number of left child
			tabRAF.seek(firstPos+2);
			int add = tabRAF.readInt();
			tabRAF.seek(add);
			tabRAF.setLength(firstPos+(numChild+1)*DavisBasePrompt.pageSize); //number of left child + root + new page 
			tabRAF.writeByte(0x0D); //leaf page
			tabRAF.writeByte(0); // number of records in the new page- which is initially 0
			tabRAF.writeShort((int) (firstPos+2*DavisBasePrompt.pageSize)); // end of the page
			tabRAF.writeInt(-1); //next page location
			tabRAF.seek(startLocation2+4); //writing address of the next leaf page on the previous leaf page
			tabRAF.writeInt(add);
			tabRAF.close();
			writeToPage(tabFile, tabFileName, columns, (int) (firstPos+DavisBasePrompt.pageSize), (int) firstPos, numRec);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	private static void updateCatTab(String tabFileName, long firstPos) {
		// TODO Auto-generated method stub
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		File fileTableCatalog = new File(tableCatalog);
		String tableName = tabFileName.substring(tabFileName.lastIndexOf("\\")+1,tabFileName.lastIndexOf(".tbl"));
		System.out.println("table name  : "+tableName);
		int rootPage = (int) firstPos;
		if (fileTableCatalog.exists()) {
			try {
				RandomAccessFile tabCatFile = new RandomAccessFile(fileTableCatalog, "rw");
				tabCatFile.seek(0);
				int numRec = tabCatFile.readByte();
				for(int i=0;i<numRec;i++) {
					tabCatFile.readByte();//delete bit
					tabCatFile.readByte();//rowid
					String tabName = tabCatFile.readUTF();
					if(tabName.equals(tableName)) {
						tabCatFile.writeInt(rootPage);
					}
					else {
						tabCatFile.readInt();
					}
				}
				tabCatFile.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}else {
			System.out.println("UNKNOWN ERROR!!");
			
		}
	}
	public static boolean writeCatalog(String tableName, ArrayList<Column> columns) {
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		String columnCatalog = Constants.catalogDataPath+"columns.tbl";
		File fileTableCatalog = new File(tableCatalog);
		File fileColumnCatalog = new File(columnCatalog);
		
			
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 *  Note that this doesn't create the table file in the correct directory structure
			 */
			RandomAccessFile tableFile = new RandomAccessFile(tableCatalog, "rw");
			//tableFile.setLength(pageSize);
			short addr = checkTable(tableName);
			
				//tableFile.seek(addr);
				//tableFile.writeByte(0);
			
			tableFile.seek(0);
			byte rec = tableFile.readByte();
			tableFile.seek(fileTableCatalog.length());
			tableFile.writeByte(0);//deletebit
			tableFile.writeByte(rec+1);//rowid
			tableFile.writeUTF(tableName);
			tableFile.writeInt(-1);
			tableFile.seek(0);
			tableFile.writeByte(rec+1);
			
			
			RandomAccessFile colFile = new RandomAccessFile(columnCatalog, "rw");
			//tableFile.setLength(pageSize);
			colFile.seek(0);
			rec = colFile.readByte();
			colFile.seek(fileColumnCatalog.length());
			for(Column colName : columns) {
				rec= (byte) (rec+1);
				colFile.writeByte(0);//deletebit
				colFile.writeInt(rec);
				colFile.writeUTF(tableName);
				colFile.writeUTF(colName.getColumnName());
				colFile.writeByte(colName.getCode());
				colFile.writeUTF(colName.isPrimaryKey());
				colFile.writeUTF(colName.isNotNullable());
				
			}
			colFile.seek(0);
			colFile.writeByte(rec);
			colFile.close();
			
			tableFile.close();
			System.out.println("Table created successfully!!");
		}
		catch(Exception e) {
			e.getMessage();
		}
		
		return true;
	}
	public static ArrayList<ReadColumn> readCatalogFile(String tableName) {

		ReadColumn rc = null;
		//String tableCatalog = catalogDataPath+"tables.tbl";
		String columnCatalog = Constants.catalogDataPath+"columns.tbl";
		//File fileTableCatalog = new File(tableCatalog);
		File fileColumnCatalog = new File(columnCatalog);
		ArrayList<ReadColumn> colArray = new ArrayList<>();
		if (fileColumnCatalog.exists()) {
			try {
				RandomAccessFile colFile = new RandomAccessFile(columnCatalog, "rw");
				colFile.seek(0);
				int numRec = colFile.readByte();
				for(int i=0;i<numRec;i++) {
					int delbit = colFile.readByte();
					colFile.readInt();
					String tabName = colFile.readUTF();
					if(tabName.equals(tableName) && delbit!=1) {
						rc = new ReadColumn();
						String colName = colFile.readUTF();
						rc.setColumnName(colName);
						byte code = colFile.readByte();
						rc.setCode(code);
						switch(code) {
						case 0x00:
							//colArray.add("onebyte");
							rc.setDataType("onebyte");
							break;
						case 0x01:
							//colArray.add("twobyte");
							rc.setDataType("twobyte");
							break;
						case 0x02:
							//colArray.add("fourbyte");
							rc.setDataType("fourbyte");
							break;
						case 0x03:
							//colArray.add("eightbyte");
							rc.setDataType("eightbyte");
							break;
						case 0x04:
							//colArray.add("tinyint");
							rc.setDataType("byte");
							break;
						case 0x05:
							//colArray.add("smallint");
							rc.setDataType("short");
							break;
						case 0x06:
							//colArray.add("int");
							rc.setDataType("int");
							break;
						case 0x07:
							//colArray.add("bigint");
							rc.setDataType("bigint");
							break;
						case 0x08:
							//colArray.add("real");
							rc.setDataType("real");
							break;
						case 0x09:
							//colArray.add("double");
							rc.setDataType("double");
							break;
						case 0x0A:
							//colArray.add("datetime");
							rc.setDataType("datetime");
							break;
						case 0x0B:
							//colArray.add("date");
							rc.setDataType("date");
							break;
						default:
							//colArray.add("text"+code);
							rc.setDataType("text");
						}
						rc.setIsPrimaryKey(colFile.readUTF());
						rc.setIsNotNullable(colFile.readUTF());
						colArray.add(rc);
					}
					else {
						colFile.readUTF();
						colFile.readByte();
						colFile.readUTF();
						colFile.readUTF();
					}
				}
				colFile.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return colArray;
		}
		
		return null;
	}
	
	public static int getRootPage(String tableName){
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		File fileTableCatalog = new File(tableCatalog);
		int rootPage = -1;
		if (fileTableCatalog.exists()) {
			try {
				RandomAccessFile tabCatFile = new RandomAccessFile(fileTableCatalog, "rw");
				tabCatFile.seek(0);
				int numRec = tabCatFile.readByte();
				for(int i=0;i<numRec;i++) {
					tabCatFile.readByte();//deletebit
					tabCatFile.readByte();
					String tabName = tabCatFile.readUTF();
					rootPage = tabCatFile.readInt();
					if(tabName.equals(tableName)) {
						break;
					}
				}
				tabCatFile.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}else {
			System.out.println("UNKNOWN ERROR!!");
			return -1;
		}
		
		return rootPage;
		
	}
	
	public static ResultSet selectWhere(String tableName, ArrayList<ReadColumn> colDTypeList, String whereCol, String whereValue, String operator) {
		
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		String rowData ="";
		ArrayList <String> result = new ArrayList<>();
		ArrayList <Integer> recordPointers = new ArrayList<>();
		ResultSet rs = new ResultSet();
		File tabFile = new File(tabFileName);
		if (tabFile.exists()) {
			try {
				RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
				tabRAF.seek(0);
				tabRAF.readByte();
				int numRec = tabRAF.readByte();
				tabRAF.readShort();
				int nextAddress = 0;
				int recPointer = 8;
				//recordPointers.add((short) recPointer);
				int page=0;
				int rowid =0;
				boolean flag = false;
				do {
					for(int rec=0;rec<numRec;rec++) {
						tabRAF.seek(recPointer);
						int recAddr = tabRAF.readShort();
						tabRAF.seek(recAddr);
						byte delbit = tabRAF.readByte();
						
						if(delbit==0) {
							rowid = tabRAF.readInt();
							
							flag=false;
							for(ReadColumn dtype : colDTypeList) {
								
								//if(dtype.getIsSelected()) {
									if(dtype.getDataType().equals("int")) {
										int value = tabRAF.readInt();
										dtype.setColData(""+value);
										if(dtype.isWhere() && operator.equals("=") && value == Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && value < Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && value > Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && value <= Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && value >= Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("byte")) {
										int value = tabRAF.readByte();
										dtype.setColData(""+value);
										if(dtype.isWhere() && operator.equals("=") && value == Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && value < Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && value > Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && value <= Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && value >= Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("short")) {
										int value = tabRAF.readShort();
										dtype.setColData(""+value);
										if(dtype.isWhere() && operator.equals("=") && value == Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && value < Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && value > Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && value <= Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && value >= Integer.parseInt(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("bigint")) {
										long value = tabRAF.readLong();
										dtype.setColData(""+value);
										if(dtype.isWhere() && operator.equals("=") && value == Long.parseLong(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && value < Long.parseLong(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && value > Long.parseLong(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && value <= Long.parseLong(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && value >= Long.parseLong(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("real")) {
										float value = tabRAF.readFloat();
										dtype.setColData(""+value);
										
										if(dtype.isWhere() && operator.equals("=") && value == Float.parseFloat(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && value < Float.parseFloat(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && value > Float.parseFloat(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && value <= Float.parseFloat(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && value >= Float.parseFloat(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("double")) {
										double value = tabRAF.readDouble();
										dtype.setColData(""+value);
										if(dtype.isWhere() && operator.equals("=") && value == Double.parseDouble(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && value < Double.parseDouble(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && value > Double.parseDouble(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && value <= Double.parseDouble(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && value >= Double.parseDouble(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("datetime")) {
										String value = tabRAF.readUTF();
										dtype.setColData(value);
										if(dtype.isWhere() && operator.equals("=") && value.equals(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && convertStringToDate(value) < convertStringToDate(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && convertStringToDate(value) > convertStringToDate(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && convertStringToDate(value) <= convertStringToDate(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && convertStringToDate(value) >= convertStringToDate(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("date")) {
										String value = tabRAF.readUTF();
										dtype.setColData(value);
										if(dtype.isWhere() && operator.equals("=") && value.equals(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<") && convertStringToDate(value) < convertStringToDate(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">") && convertStringToDate(value) > convertStringToDate(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals("<=") && convertStringToDate(value) <= convertStringToDate(whereValue.trim())) {
											flag=true;
										}
										if(dtype.isWhere() && operator.equals(">=") && convertStringToDate(value) >= convertStringToDate(whereValue.trim())) {
											flag=true;
										}
									}else if(dtype.getDataType().equals("text")) {
										String value = tabRAF.readUTF();
										dtype.setColData(value);
										if(dtype.isWhere() && value.equals(whereValue.trim())) {
											flag=true;
										}
									}
									if(dtype.getIsSelected()) {
										rowData = rowData+" "+dtype.getColData();
									}
							
							}
							if(flag == true) {
								rowData = rowid+" "+rowData;
								recordPointers.add(recAddr);
								result.add(rowData);
							}
							recPointer+=2;
							
							//System.out.println(rowData);
							rowData="";
						}
					}
					tabRAF.seek(nextAddress+4);
					nextAddress = tabRAF.readInt();
					recPointer=nextAddress+8;
				}while(nextAddress!=-1);
				tabRAF.close();
				rs.setRecordPointers(recordPointers);
				rs.setResult(result);
				
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		return rs;
		
		
	}
	public static ResultSet select(String tableName, ArrayList<ReadColumn> colDTypeList, String whereCol, String whereValue, String operator) {
		// TODO Auto-generated method stub
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		String rowData ="";
		ArrayList <String> result = new ArrayList<>();
		ArrayList <Integer> recordPointers = new ArrayList<>();
		ResultSet rs = new ResultSet();
		if(whereCol!=null) {
			rs = selectWhere(tableName , colDTypeList, whereCol , whereValue, operator);
			return rs;
		}
		File tabFile = new File(tabFileName);
		if (tabFile.exists()) {
			try {
				RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
				tabRAF.seek(0);
				tabRAF.readByte();
				int numRec = tabRAF.readByte();
				tabRAF.readShort();
				int nextAddress = 0;
				int recPointer = 8;
				//recordPointers.add(recPointer);
				int page=0;
				int rowid =0;
				do {
					for(int rec=0;rec<numRec;rec++) {
						tabRAF.seek(recPointer);
						int recAddr = tabRAF.readShort();
						tabRAF.seek(recAddr);
						byte delbit = tabRAF.readByte();
						if(delbit==0) {
							rowid = tabRAF.readInt();
							rowData = rowData+" "+rowid;
							for(ReadColumn dtype : colDTypeList) {
								
								//if(dtype.getIsSelected()) {
									if(dtype.getDataType().equals("int")) {
										dtype.setColData(""+tabRAF.readInt());
									}else if(dtype.getDataType().equals("byte")) {
										dtype.setColData(""+tabRAF.readByte());
									}else if(dtype.getDataType().equals("short")) {
										dtype.setColData(""+tabRAF.readShort());
									}else if(dtype.getDataType().equals("bigint")) {
										dtype.setColData(""+tabRAF.readLong());
									}else if(dtype.getDataType().equals("real")) {
										dtype.setColData(""+tabRAF.readFloat());
									}else if(dtype.getDataType().equals("double")) {
										dtype.setColData(""+tabRAF.readDouble());
									}else if(dtype.getDataType().equals("datetime")) {
										dtype.setColData(tabRAF.readUTF());
									}else if(dtype.getDataType().equals("date")) {
										dtype.setColData(tabRAF.readUTF());
									}else if(dtype.getDataType().equals("text")) {
										dtype.setColData(tabRAF.readUTF());
									}
									//rowData = rowData+" "+dtype.getColData();
									if(dtype.getIsSelected()) {
										rowData = rowData+" "+dtype.getColData();
									}
							
							}
							recPointer+=2;
							recordPointers.add(recAddr);
							result.add(rowData);
							//System.out.println(rowData);
							rowData="";
						}else {
							recPointer+=2;
						}
					}
					tabRAF.seek(nextAddress+4);
					nextAddress = tabRAF.readInt();
					recPointer=nextAddress+8;
					//recordPointers.add( recPointer);
				}while(nextAddress!=-1);
				tabRAF.close();
				rs.setRecordPointers(recordPointers);
				rs.setResult(result);
				
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		return rs;
	}
	public static boolean update(String tableName, ArrayList<ReadColumn> colDTypeList, String updateVal, ResultSet rs) {
		// TODO Auto-generated method stub
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		File tabFile = new File(tabFileName);
		int rowid = 0;
		String tempVal=updateVal;
		ArrayList<Integer> recordPointers= rs.recordPointers;
		if (tabFile.exists()) {
			try {
				RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
				for(int recAddr : recordPointers) {
					int tempAddr =recAddr+1+4;
					tabRAF.seek(recAddr);
					int delbit = tabRAF.readByte();
					updateVal=tempVal;
					if(delbit==1) {
						return false;
					}else {
						rowid = tabRAF.readInt();
						for(ReadColumn dtype : colDTypeList) {
							
							if(dtype.getIsSelected()) {
								if(dtype.getDataType().equals("int")) {
									//tempAddr+=4;
									tabRAF.writeInt(Integer.parseInt(updateVal));
									
								}else if(dtype.getDataType().equals("byte")) {
									//tempAddr+=1;
									tabRAF.writeByte(Byte.parseByte(updateVal));
									
								}else if(dtype.getDataType().equals("short")) {
									//tempAddr+=2;
									tabRAF.writeShort(Short.parseShort(updateVal));
								}else if(dtype.getDataType().equals("bigint")) {
									//tempAddr+=8;
									tabRAF.writeLong(Long.parseLong(updateVal));
								}else if(dtype.getDataType().equals("real")) {
									//tempAddr+=4;
									tabRAF.writeFloat(Float.parseFloat(updateVal));
								}else if(dtype.getDataType().equals("double")) {
									//tempAddr+=8;
									tabRAF.writeDouble(Double.parseDouble(updateVal));
								}else if(dtype.getDataType().equals("datetime")) {
									String tVal = tabRAF.readUTF();
									//tempAddr = tempAddr+tVal.length()+2;
									int diff = tVal.length()-updateVal.length();
									for(int i=0;i<diff ; i++) {
										updateVal=updateVal+" ";
									}
									tabRAF.seek(tempAddr);
									tabRAF.writeUTF(updateVal);
								}else if(dtype.getDataType().equals("date")) {
									String tVal = tabRAF.readUTF();
									//tempAddr = tempAddr+tVal.length()+2;
									int diff = tVal.length()-updateVal.length();
									if(diff!=0) {
										for(int i=0;i<diff ; i++) {
											updateVal=updateVal+" ";
										}
									}
									tabRAF.seek(tempAddr);
									tabRAF.writeUTF(updateVal);
								}else if(dtype.getDataType().equals("text")) {
									String tVal = tabRAF.readUTF();
									int diff = tVal.length()-updateVal.length();
									for(int i=0;i<diff ; i++) {
										updateVal=updateVal+" ";
									}
									tabRAF.seek(tempAddr);
									tabRAF.writeUTF(updateVal);								}
							}else {
								if(dtype.getDataType().equals("int")) {
									tempAddr+=4;
									dtype.setColData(""+tabRAF.readInt());
								}else if(dtype.getDataType().equals("byte")) {
									tempAddr+=1;
									dtype.setColData(""+tabRAF.readByte());
								}else if(dtype.getDataType().equals("short")) {
									tempAddr+=2;
									dtype.setColData(""+tabRAF.readShort());
								}else if(dtype.getDataType().equals("bigint")) {
									tempAddr+=8;
									dtype.setColData(""+tabRAF.readLong());
								}else if(dtype.getDataType().equals("real")) {
									tempAddr+=4;
									dtype.setColData(""+tabRAF.readFloat());
								}else if(dtype.getDataType().equals("double")) {
									tempAddr+=8;
									dtype.setColData(""+tabRAF.readDouble());
								}else if(dtype.getDataType().equals("datetime")) {
									String tVal = tabRAF.readUTF();
									tempAddr = tempAddr+tVal.length()+2;
								}else if(dtype.getDataType().equals("date")) {
									String tVal = tabRAF.readUTF();
									tempAddr = tempAddr+tVal.length()+2;
								}else if(dtype.getDataType().equals("text")) {
									String tVal = tabRAF.readUTF();
									tempAddr = tempAddr+tVal.length()+2;
								}
							}
						
						}
					}
				}
				tabRAF.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		return true;
	}
	public static boolean dropTable(String tableName) {
		// TODO Auto-generated method stub
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		File tabFile = new File(tabFileName);
		if(tabFile.exists()) {
			String tableCatalog = Constants.catalogDataPath+"tables.tbl";
			File fileTableCatalog = new File(tableCatalog);
			if (fileTableCatalog.exists()) {
				try {
					RandomAccessFile tabCatFile = new RandomAccessFile(fileTableCatalog, "rw");
					tabCatFile.seek(0);
					int addr = 0;
					int numRec = tabCatFile.readByte();
					for(int i=0;i<numRec;i++) {
						int delbit = tabCatFile.readByte();//deletebit
						addr+=1;
						tabCatFile.readByte();
						addr+=1;
						String tabName = tabCatFile.readUTF();
						addr = addr + 2 + tabName.length();
						tabCatFile.readInt();
						addr = addr +4;
						if(tabName.equals(tableName) && delbit !=1) {
							addr = addr - 7 - tabName.length();
							tabCatFile.seek(addr);
							tabCatFile.writeByte(1);
							break;
						}
					}
					tabCatFile.close();
					if(deleteColumns (tableName)) {
						if(tabFile.delete()) {
							return true;
						}
						else {
							return false;
						}
					}else {
						return false;
					}
					
				}catch(Exception e) {
					e.printStackTrace();
				}
			}else {
				System.out.println("UNKNOWN ERROR!!");
				return false;
			}
		}
		return true;
	}
	private static boolean deleteColumns(String tableName) {
		// TODO Auto-generated method stub
		String columnCatalog = Constants.catalogDataPath+"columns.tbl";
		File fileColumnCatalog = new File(columnCatalog);
		boolean flag = false;
		if (fileColumnCatalog.exists()) {
			try {
				RandomAccessFile colFile = new RandomAccessFile(columnCatalog, "rw");
				colFile.seek(0);
				String temp="";
				int addr = 0, recAddr = 1;
				int numRec = colFile.readByte();
				for(int i=0;i<numRec;i++) {
					recAddr = recAddr + addr;
					addr=0;
					colFile.readByte();//deletebit
					colFile.readInt();//rowid
					addr+=5; 
					String tabName = colFile.readUTF();
					addr = addr + 2 + tabName.length();
					if(tabName.equals(tableName)) {
						colFile.seek(recAddr);
						colFile.writeByte(1);
						colFile.seek(recAddr+addr);
						flag = true;
					}
				
					temp = colFile.readUTF();
					addr = addr + 2 + temp.length();
					colFile.readByte();
					addr = addr +1;
					temp = colFile.readUTF();
					addr = addr + 2 + temp.length();
					temp = colFile.readUTF();
					addr = addr + 2 + temp.length();
				}
				colFile.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
			
		}
		return flag;
	}
	public static void showTable() {
		// TODO Auto-generated method stub
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		File fileTableCatalog = new File(tableCatalog);
		int rootPage = -1;
		ArrayList<String> tabList = new ArrayList<>();
		if (fileTableCatalog.exists()) {
			try {
				RandomAccessFile tabCatFile = new RandomAccessFile(fileTableCatalog, "rw");
				tabCatFile.seek(0);
				int numRec = tabCatFile.readByte();
				for(int i=0;i<numRec;i++) {
					int delbit = tabCatFile.readByte();//deletebit
					tabCatFile.readByte();
					String tabName = tabCatFile.readUTF();
					rootPage = tabCatFile.readInt();
					if(delbit==0) {
						tabList.add(tabName);
					}
				}
				tabCatFile.close();
				displayTables(tabList);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}else {
			System.out.println("UNKNOWN ERROR!!");
		}
	}
	private static void displayTables(ArrayList<String> tabList) {
		// TODO Auto-generated method stub
		System.out.println("Tables");
		System.out.println("======");
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		for(String row:tabList) {
			formatter.format("%5s%5s", row.trim(), "|");
			System.out.println(sb);
			sb= new StringBuilder();
			formatter = new Formatter(sb);
		}
	}
	public static boolean isTableDel(String tableName) {
		// TODO Auto-generated method stub
		
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		File fileTableCatalog = new File(tableCatalog);
		if (fileTableCatalog.exists()) {
			try {
				RandomAccessFile tabCatFile = new RandomAccessFile(fileTableCatalog, "rw");
				tabCatFile.seek(0);
				int numRec = tabCatFile.readByte();
				for(int i=0;i<numRec;i++) {
					int delbit = tabCatFile.readByte();//deletebit
					tabCatFile.readByte();
					String tabName = tabCatFile.readUTF();
					tabCatFile.readInt();
					if(tabName.equals(tableName)) {
						if(delbit==0) {
							tabCatFile.close();
							return false;
						}else {
							tabCatFile.close();
							return true;
						}
					}
					
				}
				tabCatFile.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}else {
			System.out.println("UNKNOWN ERROR!!");
		}
		return false;
	}
	
	public static short checkTable(String tableName) throws InvalidCommandException {
		String tableCatalog = Constants.catalogDataPath+"tables.tbl";
		File fileTableCatalog = new File(tableCatalog);
		if (fileTableCatalog.exists()) {
			try {
				RandomAccessFile tabCatFile = new RandomAccessFile(fileTableCatalog, "rw");
				tabCatFile.seek(0);
				short addr = 0;
				int numRec = tabCatFile.readByte();
				for(int i=0;i<numRec;i++) {
					int delbit = tabCatFile.readByte();//deletebit
					addr+=1;
					tabCatFile.readByte();
					addr+=1;
					String tabName = tabCatFile.readUTF();
					addr = (short) (addr + 2 + tabName.length());
					tabCatFile.readInt();
					addr+=4;
					if(tabName.equals(tableName)) {
						if(delbit==0) {
							tabCatFile.close();
							throw new InvalidCommandException("Table already exists!!");
						}else {
							addr = (short) (addr - 7 - tabName.length());
							tabCatFile.close();
							return addr;
							
						}
					}
					
				}
				tabCatFile.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}else {
			System.out.println("UNKNOWN ERROR!!");
		}
		
		return 0;
		
		
	}
	public static void deleteQuery(String tableName, ArrayList<ReadColumn> colDTypeList, ResultSet rs) {
		// TODO Auto-generated method stub
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		File tabFile = new File(tabFileName);
		ArrayList<Integer> recordPointers= rs.recordPointers;
		if (tabFile.exists()) {
			try {
				RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
				for(int recAddr : recordPointers) {
					tabRAF.seek(recAddr);
					tabRAF.writeByte(1);
				}
				tabRAF.close();
				System.out.println("Query executed successfully!!");
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	public static ResultSet selectRowId(String tableName, ArrayList<ReadColumn> colDTypeList, String whereCol, String whereValue, String operator) {
		
		String tabFileName = Constants.userDataPath+tableName+".tbl";
		String rowData ="";
		ArrayList <String> result = new ArrayList<>();
		ArrayList <Integer> recordPointers = new ArrayList<>();
		ResultSet rs = new ResultSet();
		File tabFile = new File(tabFileName);
		int rootpage = getRootPage(tableName);
		int ptr=0;
		if (tabFile.exists()) {
			try {
				RandomAccessFile tabRAF = new RandomAccessFile(tabFile, "rw");
				if(rootpage!=-1) {
					tabRAF.seek(rootpage);
					tabRAF.readByte();
					int numChild = tabRAF.readByte();
					ptr = (int) (rootpage + DavisBasePrompt.pageSize)-5;
					for (int i=0;i<numChild ; i++) {
						tabRAF.seek(ptr);
						int rid = tabRAF.readByte();
						if (Integer.parseInt(whereValue) < rid) {
							ptr = tabRAF.readInt();
							break;
						}else {
							ptr = ptr-5;
						}
						
					}
				}else {
					ptr=0;
				}
				tabRAF.seek(ptr);
				tabRAF.readByte();
				int numRec = tabRAF.readByte();
				tabRAF.readShort();
				int nextAddress = 0;
				int recPointer = 8;
				//recordPointers.add((short) recPointer);
				//int page=0;
				int rowid =0;
				boolean flag = false;
				do {
					for(int rec=0;rec<numRec;rec++) {
						tabRAF.seek(recPointer);
						int recAddr = tabRAF.readShort();
						tabRAF.seek(recAddr);
						byte delbit = tabRAF.readByte();
						
						if(delbit==0) {
							rowid = tabRAF.readInt();
							flag=false;
							if (operator.equals("=") && rowid == Integer.parseInt(whereValue)) {
								flag =true;
							}
							if (operator.equals("<") && rowid < Integer.parseInt(whereValue)) {
								flag = true;
							}
							if (operator.equals(">") && rowid > Integer.parseInt(whereValue)) {
								flag= true;
							}
							if (operator.equals("<=") && rowid <= Integer.parseInt(whereValue)) {
								flag = true;
							}
							if (operator.equals(">=") && rowid >= Integer.parseInt(whereValue)) {
								flag = true;
							}
							if(flag == true) {
								for(ReadColumn dtype : colDTypeList) {
									
									if(dtype.getIsSelected()) {
										if(dtype.getDataType().equals("int")) {
											dtype.setColData(""+tabRAF.readInt());
										}else if(dtype.getDataType().equals("byte")) {
											dtype.setColData(""+tabRAF.readByte());
										}else if(dtype.getDataType().equals("short")) {
											dtype.setColData(""+tabRAF.readShort());
										}else if(dtype.getDataType().equals("bigint")) {
											dtype.setColData(""+tabRAF.readLong());
										}else if(dtype.getDataType().equals("real")) {
											dtype.setColData(""+tabRAF.readFloat());
										}else if(dtype.getDataType().equals("double")) {
											dtype.setColData(""+tabRAF.readDouble());
										}else if(dtype.getDataType().equals("datetime")) {
											dtype.setColData(tabRAF.readUTF());
										}else if(dtype.getDataType().equals("date")) {
											dtype.setColData(tabRAF.readUTF());
										}else if(dtype.getDataType().equals("text")) {
											dtype.setColData(tabRAF.readUTF());
										}
										rowData = rowData+" "+dtype.getColData();
									}
								}
								rowData = rowid+" "+rowData;
								recordPointers.add(recAddr);
								result.add(rowData);
							}
							recPointer+=2;
							
							//System.out.println(rowData);
							rowData="";
						}
					
					}
					tabRAF.seek(nextAddress+4);
					nextAddress = tabRAF.readInt();
					recPointer=nextAddress+8;
				}while(nextAddress!=-1);
				tabRAF.close();
				rs.setRecordPointers(recordPointers);
				rs.setResult(result);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		return rs;
	}


	public static long convertStringToDate(String dateParam) 
	{
		String pattern = "MM-dd-yyyy";
		SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
		try {
			Date date = dateFormat.parse(dateParam);
			return date.getTime();
		} catch (Exception e) {
			System.out.println(e);
		}
		return new Date().getTime();
	}
}
