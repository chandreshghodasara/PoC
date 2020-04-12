package net.cg.spark

class TestCases{

val spark = SparkSession
   .builder()
   .appName("SparkSessionZipsExample")
   .enableHiveSupport()
   .getOrCreate()
   
//Test that checks whether any extra department present in data on not

val deptCnt = spark.sql("select * from global_temp.Department where departmentName not in ('IT', 'INFRA', 'HR', 'ADMIN', 'FIN')").count()

if(deptCnt == 0){
println("Tast case pass: No extra department present in department table")}
else
println("Tast case fail: Extra department present in department table")

//Test that checks age boundary of employee

val empCnt = spark.sql("select * from global_temp.Employee where age<18 or age>60").count()
if(empCnt == 0)
println("Tast case pass: Employee age is between 18 and 60")
else
println("Tast case fail: Employee age is not between 18 and 60")


//Test that ctc salary boundary of employee

val empCtcRange = spark.sql("select * from global_temp.EmpFinInfo where ctc<10000 or ctc>100000").count()
if(empCtcRange == 0)
println("Tast case pass: Employee ctc is between 10000 and 100000")
else
println("Tast case fail: Employee ctc is not between 10000 and 100000")

//EmployeeSalAge

//Test that ctc salary boundary of employee

val empCtcAge = spark.sql("select * from global_temp.EmployeeSalAge where age<40 and ctc<30000").count()
if(empCtcAge == 0)
println("Tast case pass: Employee ctc is > 30000 and age >40")
else
println("Tast case fail: Employee ctc is not between 10000 and 100000")

}