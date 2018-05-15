CS 6360 - Spring 2017
Project 02 : Davisbase
Submitted by: Akash Chand
Net ID : axc173730

To run this project :
1) Browse to 'DavisBasee\src\Main' --> Run 'DavisBasePrompt.java'.
	OR
2) Import the project filesystem on to eclipse and run it.

NOTE : The data type used in this database for alphanumeric characters is "TEXT"
for example: create table test_table ( name text primarykey , age int );

NOTE : There is no space in the keywords "primarykey" and "notnull"
for example: create table test_table ( name text primarykey , age int notnull );


Table catalog files are present in :- 'DavisBasee\catalog'.
User tables are present in : 'DavisBasee\data'.

Key Features implemented :
> Database supports unique Primary key and not nullable constraints.
> Duplicate table name is not allowed.
> Number of values passed in an insert query must be equal to the number of columns in the corresponding column.
> spaces are mandatory between two tokens and also between commas ",". eg insert into test_table values ( 'column1value' , 'column2value' , integercolumn3value );
> Checks for supported data types in the CREATE TABLE query and throws error if any invalid data type is passed.
> Handled paging overflow.
> Rowid for each records is internally generated during insert operation.


Example queries :

create table test_table ( name text , age int );
insert into test_table values ( 'akash' , 25 );
select * from test_table;
select * from test_table where age <= 30;
delete from table test_table where age = 25;
show tables;
drop table test_table;
help;

