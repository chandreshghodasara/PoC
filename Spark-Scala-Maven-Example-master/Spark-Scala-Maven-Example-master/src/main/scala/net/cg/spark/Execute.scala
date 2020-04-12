package net.cg.spark

class Execute{
//Department Data Definition:

case class Department (departmentId : Int, departmentName: String)
 
//Employee Data Definition:

case class Employee (empId: Int, firstName: String, lastName: String, age: Int)

//Emp Finance info:

case class EmpFinanceInfo (empId: Int , ctc: Int, basic: Int,  pf: Int,  gratuity: Int)

//Emp Dept info:

case class EmpDeptInfo (empId: Int, departmentId: Int)

// COMMAND ----------

class ProjectUtils{
 import scala.collection.mutable.ListBuffer 
  import scala.util.Random
  //Method that create random alphanumeric word based on lenth and input character
def createRandomAlphaWord(length: Int, chars: Seq[Char]): String = {
    val tmpList = List.range(0, length)
    val charList = tmpList.map{ e => chars(util.Random.nextInt(chars.length)) }
    return charList.mkString
  }
  //Method that assign random department id from list provided as input value 
def ditributeEmpDeptData (startEmpId: Int,endEmpId: Int, depIdList: List[Int] ) : ListBuffer[EmpDeptInfo] = {
val empDeptData: ListBuffer[EmpDeptInfo] =ListBuffer()
     for ( i <- startEmpId to endEmpId ) {  
           val randomDepList1 = depIdList(Random.nextInt(depIdList.length)) //Select random value from 
           empDeptData += EmpDeptInfo(i,randomDepList1)         
     }
  return empDeptData
}
  //Method that create employee dataset
  def createEmpData(dataSize: Int) : ListBuffer[Employee] = {
     val empData: ListBuffer[Employee] =ListBuffer()
     for (i <- 1 to dataSize) {  
           
           empData += Employee(i,createRandomAlphaWord(10, ('a' to 'z') ++ ('A' to 'Z')),createRandomAlphaWord(10, ('a' to 'z') ++ ('A' to 'Z')),(math.random * (60-18) + 18).toInt)         
     }
  return empData
}
  //Method that create Employee finance dataset
  def createEmpFinanceData(dataSize: Int) : ListBuffer[EmpFinanceInfo] = {
     val empFinData: ListBuffer[EmpFinanceInfo] =ListBuffer()
     for (i <- 1 to dataSize) {  
       val tempSal = (math.random * (100000-10000) + 10000).toInt
           empFinData += EmpFinanceInfo(i,tempSal,(0.2*tempSal).toInt,(0.1*tempSal).toInt,(0.05*tempSal).toInt)         
     }
  return empFinData
}
  
}

// COMMAND ----------

def main(args: Array[String])  {

val spark = SparkSession
   .builder()
   .appName("SparkSessionZipsExample")
   .enableHiveSupport()
   .getOrCreate()
  
//Create onject of project utils to use reusable methods
val prjUtils = new ProjectUtils()

//create 5 department(IT, INFRA, HR, ADMIN, FIN)

val deptData = List(Department(1,"IT"), Department(2,"INFRA"),Department(3,"HR"), Department(4,"ADMIN"), Department(5,"FIN"))
val deptDf =  deptData.toDF

// Code to generate 1000 Emp data(unique first name), with age between 18 - 60
import scala.collection.mutable.ListBuffer

val empDataDf = prjUtils.createEmpData(1000).toDF

//generate emp finance info for each emp with CTC between 10,000 - 100,000, basic 20% CTC, PF 10% of basic, gratuity 5% of basic

val empFinDf = prjUtils.createEmpFinanceData(1000).toDF
//def main(args: Array[String]) {
    
//Distribute these emp to 5 departments, 500 emp works in 2 dept, rest 500 in 3 dept.

import scala.util.Random
val depList1 = List(1,2)
val depList2 = List(3, 4, 5)

val empDeptData1 = prjUtils.ditributeEmpDeptData(1,500,depList1).toDF // Assign 2 department to 500 employees
val empDeptData2 = prjUtils.ditributeEmpDeptData(501,1000,depList2).toDF // Assign 3 department to rest of 500 employees
val empDeptData = empDeptData1 union empDeptData2
    
//Find emp with age > 40 & ctc > 30,000
val empDataJoinEmpFin = empDataDf.join(empFinDf,empDataDf("empId") ===  empFinDf("empId"),"inner")

val specEmp = empDataJoinEmpFin.filter( empDataDf("age") > 40 && empFinDf("ctc") > 30000 )   //.select(empDataDf("*"))
//Find dept with max emp with age > 35 & gratuity < 800   

val empFinJoinEmpDept = empDataJoinEmpFin.join(empDeptData,empFinDf("empId") ===  empDeptData("empId"),"inner").join(deptDf,empDeptData("departmentId") === deptDf("departmentId"),"inner").select(deptDf("departmentId"),deptDf("departmentName")).filter( empDataDf("age") > 35 && empFinDf("gratuity") < 800 ).groupBy(deptDf("departmentId"),deptDf("departmentName")).count()

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec  = Window.orderBy(desc("count"))
val tempDf = empFinJoinEmpDept.withColumn("row_number",row_number.over(windowSpec))
val resultDf = tempDf.filter(tempDf("row_number") === 1)

//Register all datasets as table to use it in test case
spark.catalog.dropGlobalTempView("EmployeeSalAge")
spark.catalog.dropGlobalTempView("Department")
spark.catalog.dropGlobalTempView("Employee")
spark.catalog.dropGlobalTempView("EmpFinInfo")
spark.catalog.dropGlobalTempView("EmployeeAgeGra")

deptDf.createGlobalTempView("Department")
empDataDf.createGlobalTempView("Employee")
empFinDf.createGlobalTempView("EmpFinInfo")
specEmp.createGlobalTempView("EmployeeSalAge")
resultDf.createGlobalTempView("EmployeeAgeGra")

}
}

