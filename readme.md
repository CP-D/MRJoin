This is a mapreduce demo. Maven configuration is written in pom.xml.

Execute the compiled jar file 'MRJoin.jar' with command 'hadoop jar MRJoin.jar input left right field1 field2 output [argname argvalue]'.

Here is an example with two TPC-H tables:
hadoop jar MRJoin.jar /user/username/join/input customer.tbl nation.tbl 3 0 /user/username/join/output