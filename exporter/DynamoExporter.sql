SET dynamodb.throughput.write.percent=1.5;


CREATE EXTERNAL TABLE dynamo_group2_q1(
    SrcAirport string,
    Airline string,
    AvgDepDelay double )
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "group2_q1", "dynamodb.column.mapping" = "SrcAirport:SrcAirport,Airline:Airline,AvgDepDelay:AvgDepDelay");

INSERT OVERWRITE TABLE dynamo_group2_q1 SELECT * FROM group2_q1;



CREATE EXTERNAL TABLE dynamo_group2_q2(
  SrcAirport string,
  DestAirport string,
  AvgDepDelay double )
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "group2_q2", "dynamodb.column.mapping" = "SrcAirport:SrcAirport,DestAirport:DestAirport,AvgDepDelay:AvgDepDelay");

INSERT OVERWRITE TABLE dynamo_group2_q2 SELECT * FROM group2_q2;



CREATE EXTERNAL TABLE dynamo_group2_q3(
  Src_Dest_Pair string,
  Airline string,
  AvgArrDelay double )
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "group2_q3", "dynamodb.column.mapping" = "Src_Dest_Pair:Src_Dest_Pair,Airline:Airline,AvgArrDelay:AvgArrDelay");

INSERT OVERWRITE TABLE dynamo_group2_q3 SELECT * FROM group2_q3;

CREATE EXTERNAL TABLE dynamo_group3_q2(
  XYZ string,
  StartDate string,
  TotalArrDelay double,
  FirstLeg_Origin string,
  FirstLeg_Destn string,
  FirstLeg_AirlineFlightNum string,
  FirstLeg_SchedDepart string,
  FirstLeg_ArrDelay double,
  SecondLeg_Origin string,
  SecondLeg_Destn string,
  SecondLeg_AirlineFlightNum string,
  SecondLeg_SchedDepart string,
  SecondLeg_ArrDelay double )
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES ("dynamodb.table.name" = "group3_q2", "dynamodb.column.mapping" = "XYZ:XYZ,StartDate:StartDate,TotalArrDelay:TotalArrDelay,FirstLeg_Origin:FirstLeg_Origin,FirstLeg_Destn:FirstLeg_Destn,FirstLeg_AirlineFlightNum:FirstLeg_AirlineFlightNum,FirstLeg_SchedDepart:FirstLeg_SchedDepart,FirstLeg_ArrDelay:FirstLeg_ArrDelay,SecondLeg_Origin:SecondLeg_Origin,SecondLeg_Destn:SecondLeg_Destn,SecondLeg_AirlineFlightNum:SecondLeg_AirlineFlightNum,SecondLeg_SchedDepart:SecondLeg_SchedDepart,SecondLeg_ArrDelay:SecondLeg_ArrDelay ");

INSERT OVERWRITE TABLE dynamo_group3_q2 SELECT * FROM group3_q2;
