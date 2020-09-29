package com.novartis.evico.galileo.ingestion

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.databricks.spark.avro._
import com.novartis.evico.galileo.exception._
import com.novartis.evico.galileo.fourstep.DataUpdateHandler
import com.novartis.evico.galileo.tools.ReportBuilder
import org.apache.avro.Schema.Parser
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try


object ServiceNowIngestSparkApp {

  private val AVRO_FORMAT = "com.databricks.spark.avro"

  private lazy val logger = Logger.getLogger(this.getClass())

  case class CommandLineArgs(table: String = "",
                             partition: String = "",
                             partition_by: String = "",
                             spark_app_name: String = "",
                             spark_num_executors: Int = 1,
                             spark_run_hive_ctap: Boolean = false,
                             service_now_url: String = "",
                             service_now_key: String = "",
                             service_now_user: String = "",
                             service_now_pass_file: String = "",
                             service_now_limit: Int = 1000,
                             service_now_use_field_list: String = "",
                             service_now_filter: String = null,
                             service_now_range_cutoff: Int = Int.MaxValue,
                             hdfs_output_dir: String = "",
                             hdfs_schema_dir: String = "",
                             hdfs_json_dir: String = "",
                             hdfs_sys_id_file: String = "",
                             jsonDaysToKeep: Int = 3,
                             http_params_limit: Int = 100,
                             start_date: String = "",
                             end_date: String = "",
                             offset_approach: Boolean = false
                            )

  // Argument parser definition
  private val parser = new scopt.OptionParser[CommandLineArgs]("galileo-ingestion") {

    head("galileo-ingestion")

    opt[String]('t', "table")
      .required()
      .action((x, c) => c.copy(table = x))
      .text("Target table name")

    opt[String]('p', "partition")
      .required()
      .valueName("<YYYYMMDD>")
      .action((x, c) => c.copy(partition = x))
      .text("Partition specification")

    opt[String]('p', "partition-by")
      .valueName("<comma-separated list of partition columns>")
      .action((x, c) => c.copy(partition_by = x))
      .text("Partition-by specification")

    opt[String]("spark-app-name")
      .required()
      .action((x, c) => c.copy(spark_app_name = x))
      .text("Spark application name")

    opt[Int]("spark-num-executors")
      .required()
      .action((x, c) => c.copy(spark_num_executors = x))
      .text("Number of spark executors")

    opt[String]("service-now-url")
      .required()
      .action((x, c) => c.copy(service_now_url = x))
      .text("Service Now base url")

    opt[String]("service-now-key")
      .required()
      .action((x, c) => c.copy(service_now_key = x))
      .text("Service Now API Key")

    opt[String]("service-now-user")
      .required()
      .action((x, c) => c.copy(service_now_user = x))
      .text("Service Now username")

    opt[String]("hdfs-json-dir")
      .action((x, c) => c.copy(hdfs_json_dir = x))
      .text("Path to HDFS folder with data in JSON format")

    opt[String]("hdfs-sys-ids-file")
      .action((x, c) => c.copy(hdfs_sys_id_file = x))
      .text("Path to HDFS to file with sys_id separated by comma")

    opt[String]("service-now-pass-file")
      .required()
      .valueName("<path>")
      .action((x, c) => c.copy(service_now_pass_file = x))
      .text("Service Now password file")

    opt[Int]("service-now-limit")
      .required()
      .action((x, c) => c.copy(service_now_limit = x))
      .text("Service Now limit of records per page")

    opt[String]("service-now-filter")
      .required()
      .action((x, c) => c.copy(service_now_filter = x))
      .text("Service Now filter for records")

    opt[String]("service-now-use-field-list")
      .action((x, c) => c.copy(service_now_use_field_list = x))
      .text("Service Now field list")

    opt[Int]("service-now-range-cutoff")
      .action((x, c) => c.copy(service_now_range_cutoff = x))
      .text("name of the database where hive tables will reside")

    opt[String]("hdfs-output-dir")
      .required()
      .valueName("<path>")
      .action((x, c) => c.copy(hdfs_output_dir = x))
      .text("HDFS dir to store output data")

    opt[String]("hdfs-schema-dir")
      .required()
      .valueName("<path>")
      .action((x, c) => c.copy(hdfs_schema_dir = x))
      .text("HDFS dir where avro schemas are stored")

    opt[Int]("json-days-to-keep")
      .action((x, c) => c.copy(jsonDaysToKeep = x))
      .text("Number of days to keep raw JSON input on HDFS. Default is 3 days")

    opt[Int]("http_params_limit")
      .action((x, c) => c.copy(http_params_limit = x))
      .text("Number of limit for HTTP request. Default is 100")

    opt[String]("start-date")
      .required()
      .action((x, c) => c.copy(start_date = x))
      .text("Start date for ingestion")

    opt[String]("end-date")
      .required()
      .action((x, c) => c.copy(end_date = x))
      .text("End date for ingestion")

    opt[Boolean]("offset-approach")
      .action((x, c) => c.copy(offset_approach = x))
      .text("Offset ingestion approach")

    help("help").text("prints this usage text")

  }

  def main(args: Array[String]): Unit = {

    parser.parse(args, new CommandLineArgs()) match {

      case None => parser.showUsage

      case Some(arguments) => {

        val table = arguments.table
        val partition = arguments.partition
        val partition_by = arguments.partition_by.split(',').filter(!_.isEmpty)
        val num_executors = arguments.spark_num_executors
        val limit = arguments.service_now_limit
        val take = arguments.service_now_range_cutoff
        val schema_hdfs_path = arguments.hdfs_schema_dir
        val output_hdfs_path = arguments.hdfs_output_dir

        val daysToKeep = arguments.jsonDaysToKeep
        val httpParamsLimit = arguments.http_params_limit

        val hdfsJsonDir = arguments.hdfs_json_dir
        val hdfsSysIdFile = arguments.hdfs_sys_id_file
        // Get ServiceNow password from secured file
        val password = Source.fromFile(arguments.service_now_pass_file)
          .getLines()
          .mkString("")
          .trim()

        // Capture runtime timestamp
        val ts = System.currentTimeMillis()

        val startDate = arguments.start_date
        val endDate = arguments.end_date
        val offsetApproach = arguments.offset_approach

        // Start Spark application
        val spark = SparkSession.builder()
          //          .master("local[10]")
          .appName(arguments.spark_app_name)
          .getOrCreate()

        logger.info(s"Processing for table: [ ${table} ] and partition: [ ${partition} ] started!")

        val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
        val fileSystem = FileSystem.get(hadoopConfiguration)

        // Read AVRO schema from HDFS
        val schemaFile = fileSystem.open(new Path(s"${schema_hdfs_path}/${table}.avsc"))

        // Parse avro schema
        val avroSchema = new Parser().parse(schemaFile)
        //Create schema for dataset with error rows where all columns are string
        val stringSchema = StructType(avroSchema.getFields.toList.map(f => StructField(f.name(), StringType, nullable = true)))

        // Set sysparmFields argument based on inputs
        val sysparmFields = if (arguments.service_now_use_field_list.length()>0) {
          Option(arguments.service_now_use_field_list)
        } else {
          None
        }
         
        logger.info(s"Generated list of fields to process:  ${sysparmFields}")
        // Create ServiceNow HTTP client pased on input arguments (Driver-local only)
        lazy val client = new ServiceNowHttpClient(
          url = arguments.service_now_url,
          key = arguments.service_now_key,
          username = arguments.service_now_user,
          password = password
        )

        val reportBuilder = new ReportBuilder

        var countToProcess = 0
        import spark.implicits._
        var jsonRows: RDD[String] = null

        def fetchDataRangeByTimeChunks(ranges: RDD[(String, String)], queryFilter: String) = {
          ranges.mapPartitions { partition =>
            val client = new ServiceNowHttpClient(url = arguments.service_now_url,
              key = arguments.service_now_key,
              username = arguments.service_now_user,
              password = password)
            val reportBuilder = new ReportBuilder
            partition.flatMap { range =>
              var finish = false
              var startDate = range._1
              val resultList = new ListBuffer[String]
              var filter = ""
              var last_sys_id=""
              while (!finish) {
                filter = queryFilter + s"sys_id>=$last_sys_id^sys_updated_on>=$startDate^sys_updated_on<=${range._2}"
                logger.info(s"Current filter query applied on this cycle :  ${filter}")
                val url1 = client.composeURL(table, None, limit, sysparmFields, Option(filter)) + "&sysparm_suppress_pagination_header=true"
                val res = client.processRequest(url1)
                val resBuffer = res.toBuffer
                logger.info(s"response retuned in this cycle/executor :  ${res}")
                if (resBuffer.isEmpty) {
                  finish = true
                } else {
                  //startDate = resBuffer.last.get("sys_updated_on").asText()
                  last_sys_id = resBuffer.last.get("sys_id").asText()
                  logger.info(s"Last SYS_ID pulled up from last record of previous cycle :  ${last_sys_id}")
                  resBuffer.foreach(r => resultList.add(r.toString))
                  var resultbuffer = resBuffer.length
                  logger.info(s"Records returned from this cycle :  ${resultbuffer}") 
                  //value 2 is added instead of limit of 900 to fetch till last record
                  if (resBuffer.length < 2) {
                    finish = true
                  }
                }
              }
              reportBuilder
                .withAction("executorIngestion")
                .withKey("table", table)
                .withKey("startRange", range._1)
                .withKey("endRange", range._2)
                .withKey("processedRecords", resultList.size)
              logger.info(reportBuilder.build())
              resultList
            }
          }
        }

        def fetchDataRangeByOffsets(ranges: RDD[Range], queryFilter: String) = {
          ranges.mapPartitions { partition =>
            val client = new ServiceNowHttpClient(url = arguments.service_now_url,
              key = arguments.service_now_key,
              username = arguments.service_now_user,
              password = password)
            partition.flatMap { range =>
              // Process each range in loop (ideally there should be 1 range per 1 executor)
              range.grouped(limit).take(take).flatMap { r =>
                // Divide range into smaller chunks (10000 records default) and process each record in chunk individually
                val url1 = client.composeURL(table, Option(r.head), r.length, sysparmFields, Option(queryFilter)) + "&sysparm_suppress_pagination_header=true"
                val res = client.processRequest(url1)
                res.map(r => r.toString)
              }
            }
          }
        }

        def fetchDataSysIds(sysIds: Array[String], queryFilter: Option[String]) = {
          val numPartitions = sysIds.length / httpParamsLimit + 1
          logger.info(s"Number of partitions: $numPartitions")
          spark.sparkContext.parallelize(sysIds).repartition(numPartitions).mapPartitions(partition => {
            val client = new ServiceNowHttpClient(url = arguments.service_now_url, key = arguments.service_now_key, username = arguments.service_now_user, password = password)
            val sysIds = partition.mkString(",")
            val url1 = client.composeURL(table, Option.apply(0), limit, sysparmFields, Option(s"sys_idIN$sysIds")) + "&sysparm_suppress_pagination_header=true"
            client.processRequest(url1).map(r => r.toString)
          })
        }

        var ranges: RDD[(String, String)] = spark.sparkContext.emptyRDD
        var queryFilter: Option[String] = null

        if (hdfsSysIdFile.isEmpty) {
          queryFilter = Option(Utils.nullString(arguments.service_now_filter))
          val defaultFilter = queryFilter match {
            case Some(f) => f + "^"
            case None => ""
          }
          val filter = defaultFilter + s"sys_updated_on>=$startDate^sys_updated_on<=$endDate"
          if(table != "metric_instance") countToProcess = client.getNumRecords(table, sysparmFields, Option(filter))

          logger.info(s"$table: Approximately num of records from '${client.X_TOTAL_COUNT_HEADER}' HTTP header : $countToProcess")
          logger.info(s"Number of executors: $num_executors")
          logger.info(s"Start date: $startDate")
          logger.info(s"End date: $endDate")
          jsonRows =
            if (offsetApproach) {
              val ranges = spark.sparkContext.parallelize(Utils.splitRange(0 to countToProcess, num_executors))
              logger.info(s"Filter is: $filter")
              fetchDataRangeByOffsets(ranges, filter)
            } else {
              val splittedRanges = Utils.splitDateRange(startDate, endDate, num_executors)
              logger.info(s"Ranges are: $splittedRanges")
              ranges = spark.sparkContext.parallelize(splittedRanges)
              fetchDataRangeByTimeChunks(ranges, defaultFilter)
            }
        } else {
          val sysIds = spark.read.text(hdfsSysIdFile).map(_.getString(0).split(",")).reduce((a, b) => a)
          jsonRows = fetchDataSysIds(sysIds, queryFilter)
        }

        //persisting to not request REST API twice
        jsonRows.persist(StorageLevel.MEMORY_AND_DISK)

        //writing raw JSON onto HDFS
        writeJSON(table, output_hdfs_path, daysToKeep, fileSystem, jsonRows)


//This is to enable the fix for null column -begin
        //spark.sqlContext.read.json(jsonRows.toDS()).createTempView(table)
//This is to enable the fix for null column -end

//New code for null column -begin
        val JsonDF=spark.sqlContext.read.json(jsonRows.toDS())
        JsonDF.na.fill("",JsonDF.columns).createTempView(table)
//New code for null column -end

        //getting list of fields from schema
        val expectedColumns = avroSchema.getFields.map(f => f.name())

        //selecting to get list of columns exist in current JSON response
        val rawDataAllColumns: DataFrame = spark.sql(s"select * from ${table} where 0=1")
        val existColumns = expectedColumns.flatten(column => Try(rawDataAllColumns(column)).toOption).map(c => c.toString())

        //showing what missed in JSON and exists in HIVE table
        val columnDiff = expectedColumns.diff(existColumns)
        if (!columnDiff.isEmpty) logger.warn(s">>>>> Following columns exist in EVICO raw layer and does not exist in response from REST API (default values will be set): " +
          s"${columnDiff.mkString(", ")}")

        //putting empty values in missing columns
        val selectStatementColumns = mutable.ArrayBuffer[String]()
       expectedColumns.map(c => {
          if (existColumns.contains(c)) {
            selectStatementColumns += c
          } else {
		   
		   selectStatementColumns += s"'' as ${c}"            
          }
        })
        
        //selecting from JSON taking into account missing columns
        val rawDataExistColumns = spark.sql(s"select ${selectStatementColumns.mkString(",")} from ${table}")

        //converting schema to StructType
        val schema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
        //creating encoder for future processing of converted row to match HIVE table schema
        val convertedRowEncoder = RowEncoder.apply(schema)
        //creating encoder for future processing of error rows where all columns are string to represent original wrong value
        val stringRowEncoder = RowEncoder.apply(stringSchema)
        //creating tuple from encoders where _1 is with converted row and _2 is with error row
        val encoders: Encoder[(Row, Row)] = Encoders.tuple(convertedRowEncoder, stringRowEncoder)

        val ERR_MSG = "!ERROR! row#%d '%s':'%s' "

        //transforming Schema to serializable form
        case class FieldSchema(name: String, fieldType: String, fieldLogicalType: String)
        val fields = avroSchema.getFields.toList.map(f => {
          var logicalType: String = null
          if (f.schema().getLogicalType != null) logicalType = f.schema().getLogicalType.getName
          FieldSchema(f.name(), f.schema().getType.getName, logicalType)
        })

        //preparing list with empty fields to put in _2 in tuple in case row converted successful
        val emptyFields: List[String] = fields.map(f => null)

        //to count number of current processing row
        val rowNumber = spark.sparkContext.longAccumulator("rowNumber")

        //main processing algorithm
        val processedRows = rawDataExistColumns.map(row => {

          //var to keep state
          var caughtError = false
          val seq = fields.map(f => {
            //getting field position and in case of field does not exist returning -1.
            //means it is in JSON and not in Schema
            def getFieldPos(row: Row, name: String): Int = {
              try {
                row.fieldIndex(f.name)
              } catch {
                case e: IllegalArgumentException => -1
              }
            }

            var convertedVal: Any = null
            try {
              val fieldPos = getFieldPos(row, f.name)
              //if field is in Schema and JSON casting to required type
              if (fieldPos >= 0) convertedVal = DataTypeConverterEx.convert(row.getString(row.fieldIndex(f.name)), f.name, f.fieldType, f.fieldLogicalType)
              //if not then getting default value for the type
              else convertedVal = DataTypeConverterEx.defaultValue(f.fieldType, f.fieldLogicalType)
            } catch {
              case e: IllegalArgumentException => {
                logger.error(ERR_MSG.format(rowNumber.value, f.name, f.fieldType) + e.getMessage)
                caughtError = true
              }
              case e: IntConvertException => {
                logger.error(ERR_MSG.format(rowNumber.value, f.name, f.fieldType) + e.getMessage)
                caughtError = true
              }
              case e: LongConvertException => {
                logger.error(ERR_MSG.format(rowNumber.value, f.name, f.fieldType) + e.getMessage)
                caughtError = true
              }
              case e: BooleanConvertException => {
                logger.error(ERR_MSG.format(rowNumber.value, f.name, f.fieldType) + e.getMessage)
                caughtError = true
              }
              case e: DoubleConvertException => {
                logger.error(ERR_MSG.format(rowNumber.value, f.name, f.fieldType) + e.getMessage)
                caughtError = true
              }
              case e: DateFormatException => {
                logger.error(ERR_MSG.format(rowNumber.value, f.name, f.fieldType) + e.getMessage)
                caughtError = true
              }
              case e: Exception => {
                caughtError = true
                logger.error(s"Failed to convert field '${f.name}' of type (${f.fieldType}, ${f.fieldLogicalType}", e)
              }
            }
            //if any error happened during type casting then return default value for the type
            if (caughtError) {
              DataTypeConverterEx.defaultValue(f.fieldType, f.fieldLogicalType)
            } else {
              convertedVal
            }
          })
          rowNumber.add(1L)
          if (caughtError) {
            //return converted row as _1 with default values set for some fields AND original row as _2 with all columns as String
            (Row.fromSeq(seq), row)
          } else {
            //return converted row as _1 AND empty row as _2 because no errors happened during types casting
            (Row.fromSeq(seq), Row(emptyFields: _*))
          }
        })(encoders)

        //persisting to not execute above operation twice
        processedRows.persist(StorageLevel.MEMORY_AND_DISK)

        //storing error rows as Parquet
        val errorRows = processedRows.map(_._2)(stringRowEncoder).na.drop()
        errorRows
          .write
          .partitionBy(partition_by: _*)
          .option("recordName", avroSchema.getName)
          .option("recordNamespace", avroSchema.getNamespace)
          .mode(SaveMode.Overwrite)
          .parquet(s"$output_hdfs_path/${table}_err/dt=$partition")

        //storing converted rows as AVRO (not Parquet because of historical reasons)
        val convertedRows = processedRows.map(_._1)(convertedRowEncoder)
        convertedRows
          .write
          .partitionBy(partition_by: _*)
          .option("recordName", avroSchema.getName)
          .option("recordNamespace", avroSchema.getNamespace)
          .mode(SaveMode.Overwrite)
          .avro(s"$output_hdfs_path/$table/dt=$partition")

        val countProcessed = convertedRows.count()
        val countErr = errorRows.count()
        val key = table match {
          case "u_nvs_it_deletion_log" => DataUpdateHandler.DELETIONS_PK_FIELD_NAME
          case _ => DataUpdateHandler.UPDATES_PK_FIELD_NAME
        }
        val countUniqueSysIds = convertedRows.select(key).distinct().count()

        reportBuilder
          .withAction("ingestion")
          .withKey("table", table)
          .withKey("recordsCountFromHeader", countToProcess)
          .withKey("processedRecords", countProcessed)
          .withKey("processingTime", System.currentTimeMillis() - ts)
          .withKey("uniqueCountInDelta", countUniqueSysIds)
          .withKey("errorRecords", countErr)

        logger.info(reportBuilder.build())

        val longDateSDF = new SimpleDateFormat("yyyyMMdd_HHmmss")
        val shortDateSDF = new SimpleDateFormat("yyyyMMdd")
        val curLongDate = shortDateSDF.format(new Date(System.currentTimeMillis()))

        val logFileName = s"$output_hdfs_path/logs/log_$curLongDate"
        val logFileNamePath = new Path(logFileName)

        var logFileStream: FSDataOutputStream = null
        try {
          if (!fileSystem.exists(logFileNamePath)) {
            fileSystem.create(logFileNamePath).close()
          }
          logFileStream = fileSystem.append(logFileNamePath)
          logFileStream.writeBytes(longDateSDF.format(new Date(System.currentTimeMillis())) + reportBuilder.build())
        } catch {
          case e: Exception => logger.error(e)
        } finally {
          if (logFileStream != null) logFileStream.close()
        }

        if (!hdfsJsonDir.isEmpty & jsonRows != null) jsonRows.unpersist()
        processedRows.unpersist()

        // Stop Spark application
        spark.close()
        ServiceNowHttpClient
      }
    }
  }

  private def writeJSON(table: String, output_hdfs_path: String, daysToKeep: Int, fileSystem: FileSystem, jsonRows: RDD[String]): Path = {
    var jsonFile: Path = null
    try {
      val jsonDir = new Path(s"${output_hdfs_path}/json")
      if (!fileSystem.exists(jsonDir)) fileSystem.mkdirs(jsonDir)
      val dts_sdf = new SimpleDateFormat("yyyyMMdd")
      val currentFormattedDate = dts_sdf.format(new Date(System.currentTimeMillis()))

      val c = Calendar.getInstance()
      c.add(Calendar.DAY_OF_MONTH, -daysToKeep)
      val folderToRemove = new Path(s"${jsonDir.toString}/${table}_${dts_sdf.format(c.getTime)}")
      if (fileSystem.exists(folderToRemove)) fileSystem.delete(folderToRemove, true)

      jsonFile = new Path(s"${jsonDir.toString}/${table}_${currentFormattedDate}")
      if (fileSystem.exists(jsonFile)) {
        logger.warn(s">>===Folder '${jsonFile}' exists. Deleting...")
        fileSystem.delete(jsonFile, true)
      }

      logger.info(s">>===Writing '${table}' as JSON to ${jsonFile}===>>")
      jsonRows.saveAsTextFile(jsonFile.toString)
    } catch {
      case ex: Exception => logger.error(s">>>>>>>>>>>Failed to write '${table}' as JSON to HDFS", ex)
    }
    jsonFile
  }
}
