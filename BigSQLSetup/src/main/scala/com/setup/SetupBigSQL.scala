package com.setup

import scala.io.{Source, StdIn}
import scala.sys.process._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{breakable, break}
import java.io.{InputStream, ByteArrayOutputStream, FileWriter, PrintWriter}
import java.io.{File, FileWriter, BufferedWriter}



object SetupBigSQL {

  def main(args: Array[String]): Unit = setup(args)
  
  private def setup(args: Array[String]){
    println("\n\n Setting up the BigSQL (Estimated time for completion is 35-60 mins)")
    Thread.sleep(1 * 1000)

    if (args.length == 0) {
       print("\n\n Provide the URL for BigSQL Repo : ")
       val bigsqlRepoURL = scala.io.StdIn.readLine()
       //val bigsqlRepoURL = "http://birepo-build.svl.ibm.com/repos/BigSQL/RHEL7/x86_64/5.0.4.0/48/IBM-Big_SQL-5_0_4_0.el7.x86_64.rpm"

       print("\n\n Provide the Hostnames for the BigSQL Worker Nodes to be installed (more than one hostname delimited by comma): ")
       val bigsqlworkers = StdIn.readLine()

       val sp = new SetupBigSQL(bigsqlRepoURL, bigsqlworkers)
       sp.process()

     } else {
       
       val bigsqlRepoURL = args(0).trim
       val bigsqlworkers = args(1).trim
       val sp = new SetupBigSQL(bigsqlRepoURL, bigsqlworkers)
       sp.process()
     }


  }
}


class SetupBigSQL(bigsqlRepoURL: String, bigsqlworkers: String) {

  var infraCommands : List[(String, String)] = List.empty
  var headnodeCommands : List[(String, String)] = List.empty
  var workerCommands : List[(String, String)] = List.empty

  def process(){
    loadinfraCommands()
    var bigsqlHead = "hostname -f".!!.trim
    print("\n\n Downloding the BigSQL RPMs")
    executeCommands(infraCommands)
    var nodeArr = bigsqlworkers.split(",").map(_.trim())
    createWorkerConfiguration("/tmp/CreateClusterSetup/bigsqlWorkers", nodeArr)

    executeCommands(Seq("bash", "-c", s"""ssh-keyscan -t ecdsa $bigsqlHead >> /home/bigsql/.ssh/known_hosts"""))

    print("\n\n Setting up the headnode")
    loadHeadnodeCommands()
    executeCommands(headnodeCommands)
 
    val filename = "/tmp/CreateClusterSetup/bigsqlWorkers"
    for (node <- Source.fromFile(filename).getLines) {
        print(s"\n\n Setting up workernode -> $node")

        var out = executeCommands(Seq("bash", "-c", s"""ssh-keyscan -t ecdsa $node >> /home/bigsql/.ssh/known_hosts"""))

        loadWorkernodeCommands(node)
        executeCommands(workerCommands)
     }


    
    val nodes = collection.mutable.ArrayBuffer(nodeArr: _*)
    nodes += bigsqlHead 
    print(s"\n\n Setting up SSH between bigsql user")
    setupSSH(nodes)

    var command = s"""curl -i -u admin:admin -H 'X-Requested-By:ambari' -X GET http://localhost:8080/api/v1/clusters/ | grep \"cluster_name\" | cut -c27- | head -c -3"""
    var commandOut = executeCommands(Seq("bash", "-c", command))
    var clusterName = commandOut._2.trim

    print("\n\n Setting up BigSQL Service for instalation")
    setupBigSQLService(clusterName)
    print("\n\n Installing the BigSQL Service. You can see the progress of the installation from Ambari UI")
    Thread.sleep(25 * 60 * 1000)
    checkBigSQLServiceInstalledOrStarted(clusterName)
    recheckBIGSQLServiceStarted(clusterName)

   print("\n\n BigSQL Installation completed !!!!! \n\n\n")


  }


  private def checkBigSQLServiceInstalledOrStarted(clusterName :String) {

    var loopBreak = 20
    var index = 0
    index = 0
    breakable {
        do {
          var comm = s"""curl -u admin:admin -H "X-Requested-By: ambari" -i -X GET http://localhost:8080/api/v1/clusters/$clusterName/services/BIGSQL  | grep \"INSTALLED\\|STARTED\""""
          var (exitValue, stdout, stderr) = executeCommands(Seq("bash", "-c", comm))
          if((exitValue == 0 && ! stdout.isEmpty ) || index == loopBreak){
            print("\n\n BigSQL Installation completed !!! ")
            break
          }
          Thread.sleep(2 * 60 * 1000)
          index += 1
            

        }while(true)
      }
    
  }

  private def recheckBIGSQLServiceStarted(clusterName :String) {

      breakable {

        var comm = s"""curl -u admin:admin -H "X-Requested-By: ambari" -i -X GET http://localhost:8080/api/v1/clusters/$clusterName/services/BIGSQL  | grep \"STARTED\""""
        var (exitValue, stdout, stderr) = executeCommands(Seq("bash", "-c", comm))
        if(exitValue == 0 && ! stdout.isEmpty ){
          break
        }

        print("\n\n Starting the BigSQL service")
        comm = s"""curl -u admin:admin -H "X-Requested-By: ambari" -i -X PUT -d '{"ServiceInfo": {"state" : "STARTED"}}'  http://localhost:8080/api/v1/clusters/$clusterName/services/BIGSQL"""
        executeCommands(Seq("bash", "-c", comm))
        Thread.sleep(10 * 1000)

      }
    
  }

  private def setupBigSQLService(clusterName :String){

    var bigsqlHead = "hostname -f".!!.trim

    var command = s"""curl -i -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '{\"ServiceInfo\":{\"service_name\":\"BIGSQL\"}}' http://localhost:8080/api/v1/clusters/$clusterName/services"""
    var commandOut = executeCommands(Seq("bash", "-c", command))
    Thread.sleep(1 * 1000)

    command = s"""curl -i -u admin:admin -H 'X-Requested-By: ambari' -X POST http://localhost:8080/api/v1/clusters/$clusterName/services/BIGSQL/components/BIGSQL_HEAD"""
    commandOut = executeCommands(Seq("bash", "-c", command))
    Thread.sleep(1 * 1000)


    command = s"""curl -i -u admin:admin -H 'X-Requested-By: ambari' -X POST http://localhost:8080/api/v1/clusters/$clusterName/services/BIGSQL/components/BIGSQL_WORKER"""
    commandOut = executeCommands(Seq("bash", "-c", command))
    Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     {
        \"type\":\"bigsql-conf\",
	\"tag\":\"version1\", 
        \"properties_attributes\" : { },
        \"properties\" : {
          \"biginsights.stats.auto.analyze.post.load\" : \"ONCE\",
          \"biginsights.stats.auto.analyze.task.retention.time\" : \"1MONTH\",
          \"scheduler.autocache.poolsize\" : \"0\",
          \"scheduler.cache.exclusion.regexps\" : \"None\",
          \"bigsql.load.jdbc.jars\" : \"/tmp/jdbcdrivers\",
          \"scheduler.parquet.rgSplit.minFileSize\" : \"2147483648\",
          \"fs.sftp.impl\" : \"org.apache.hadoop.fs.sftp.SFTPFileSystem\",
          \"scheduler.client.request.timeout\" : \"120000\",
          \"scheduler.service.timeout\" : \"3600000\",
          \"scheduler.parquet.rgSplits\" : \"true\",
          \"scheduler.cache.splits\" : \"true\",
          \"biginsights.stats.auto.analyze.newdata.min\" : \"50\",
          \"biginsights.stats.auto.analyze.concurrent.max\" : \"1\",
          \"scheduler.tableMetaDataCache.timeToLive\" : \"1200000\",
          \"scheduler.minWorkerThreads\" : \"8\",
          \"javaio.textfile.extensions\" : \".snappy,.bz2,.deflate,.lzo,.lz4,.cmx\",
          \"scheduler.maxWorkerThreads\" : \"1024\",
          \"scheduler.autocache.ddlstate.file\" : \"/var/ibm/bigsql/logs/.AutoCacheDDLStateDoNotDelete\",
          \"biginsights.stats.auto.analyze.post.syncobj\" : \"DEFERRED\",
          \"scheduler.autocache.poolname\" : \"autocachepool\",
          \"scheduler.client.request.IUDEnd.timeout\" : \"600000\",
          \"scheduler.java.opts\" : \"-Xms512M -Xmx2G\",
          \"scheduler.tableMetaDataCache.numTables\" : \"1000\"
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)



   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     { 
        \"type\":\"bigsql-slider-flex\", 
        \"tag\":\"version1\", 
        \"properties\":{ 
          \"bigsql_capacity\" : \"50\" 
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     { 
        \"type\":\"bigsql-head-env\", 
        \"tag\":\"version1\", 
        \"properties_attributes\":{ 
          \"final\" : {
            \"fs.defaultFS\" : \"true\"
          }
        }, 
        \"properties\":{ 
          \"bigsql_active_primary\" : \"head_node\" 
        } 
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     {
        \"type\":\"bigsql-env\",
	\"tag\":\"version1\", 
        \"properties_attributes\" : { },
        \"properties\" : {
          \"bigsql_hdfs_poolsize\" : \"0\",
          \"db2_fcm_port_number\" : \"28051\",
          \"apply_best_practice_configuration_changes\" : \"true\",
          \"bigsql_java_heap_size\" : \"2048\",
          \"enable_impersonation\" : \"false\",
          \"bigsql_ha_port\" : \"20008\",
          \"enable_auto_metadata_sync\" : \"true\",
          \"enable_metrics\" : \"true\",
          \"bigsql_hdfs_poolname\" : \"autocachepool\",
          \"db2_port_number\" : \"32051\",
          \"scheduler_service_port\" : \"7053\",
          \"bigsql_mln_inc_dec_count\" : \"1\",
          \"public_table_access\" : \"false\",
          \"bigsql_initial_install_mln_count\" : \"1\",
          \"scheduler_admin_port\" : \"7054\",
          \"bigsql_log_dir\" : \"/var/ibm/bigsql/logs\",
          \"bigsql_db_path\" : \"/var/ibm/bigsql/database\",
          \"enable_yarn\" : \"false\",
          \"bigsql_resource_percent\" : \"25\",
          \"enable_auto_log_prune\" : \"true\",
          \"bigsql_continue_on_failure\" : \"false\",
          \"dfs.datanode.data.dir\" : \"/hadoop/bigsql\"
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     {
        \"type\":\"bigsql-users-env\",
        \"tag\":\"version1\", 
        \"properties_attributes\" : { 
		    \"ambari_user_password\" : {
              \"toMask\" : \"false\"
            },
            \"bigsql_user_password\" : {
              \"toMask\" : \"false\"
            }

		},
        \"properties\" : {
          \"ambari_user_login\" : \"admin\",
          \"ambari_user_password\" : \"admin\",
          \"bigsql_user_id\" : \"2824\",
          \"enable_ldap\" : \"false\",
          \"bigsql_user\" : \"bigsql\",
          \"bigsql_user_password\" : \"bigsql\",
          \"bigsql_admin_group_name\" : \"bigsqladm\",
          \"bigsql_admin_group_id\" : \"43210\",
          \"bigsql_setup_ssh\" : \"false\"
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     {
        \"type\":\"bigsql-log4j\",
	\"tag\":\"version1\", 		
        \"properties_attributes\" : { },
        \"properties\" : {
          \"bigsql_log_number_of_backup_files\" : \"15\",
          \"bigsql_scheduler_log4j_content\" : \"\n# Logging is expensive, so by default we only log if the level is >= WARN.\n# If you want any other logging to be done, you need to set the below 'GlobalLog' logger to DEBUG,\n# plus any other logger settings of interest below.\nlog4j.logger.com.ibm.biginsights.bigsql.scheduler.GlobalLog=WARN\n\n# Define the loggers\nlog4j.rootLogger=WARN,verbose\nlog4j.logger.com.ibm.biginsights.bigsql.scheduler.server.RecurringDiag=INFO,recurringDiagInfo\nlog4j.additivity.com.ibm.biginsights.bigsql.scheduler.server.RecurringDiag=false\n\n# Suppress unwanted messages\n#log4j.logger.javax.jdo=FATAL\n#log4j.logger.DataNucleus=FATAL\n#log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL\n\n# Verbose messages for debugging purpose\n#log4j.logger.com.ibm=ALL\n#log4j.logger.com.thirdparty.cimp=ALL\n#log4j.logger.com.ibm.biginsights.bigsql.io=WARN\n#log4j.logger.com.ibm.biginsights.bigsql.hbasecommon=WARN\n#log4j.logger.com.ibm.biginsights.catalog.hbase=WARN\n\n# Uncomment this to print table-scan assignments (node-number to number-of-blocks)\n#log4j.logger.com.ibm.biginsights.bigsql.scheduler.Assignment=DEBUG\n\n# setup the verbose logger\nlog4j.appender.verbose=org.apache.log4j.RollingFileAppender\nlog4j.appender.verbose.file={{bigsql_log_dir}}/bigsql-sched.log\nlog4j.appender.verbose.layout=org.apache.log4j.PatternLayout\nlog4j.appender.verbose.layout.ConversionPattern=%d{ISO8601} %p %c [%t] : %m%n\nlog4j.appender.verbose.MaxFileSize={{bigsql_log_max_backup_size}}MB\nlog4j.appender.verbose.MaxBackupIndex={{bigsql_log_number_of_backup_files}}\n\n# setup the recurringDiagInfo logger\nlog4j.appender.recurringDiagInfo=org.apache.log4j.RollingFileAppender\nlog4j.appender.recurringDiagInfo.file={{bigsql_log_dir}}/bigsql-sched-recurring-diag-info.log\nlog4j.appender.recurringDiagInfo.layout=org.apache.log4j.PatternLayout\nlog4j.appender.recurringDiagInfo.layout.ConversionPattern=%d{ISO8601} %p %c [%t] : %m%n\nlog4j.appender.recurringDiagInfo.MaxFileSize=10MB\nlog4j.appender.recurringDiagInfo.MaxBackupIndex=1\n\n# Setting this to DEBUG will cause ALL queries to be traced, INFO will cause\n# only sessions that specifically request it to be traced\nlog4j.logger.bigsql.query.trace=INFO\n\n# Silence hadoop complaining about forcing hive properties\nlog4j.logger.org.apache.hadoop.conf.Configuration=ERROR\n# Silence warnings about non existing hive properties\nlog4j.logger.org.apache.hadoop.hive.conf.HiveConf=ERROR\n\n# Uncomment and restart bigsql to get the details. Use INFO for less detail, DEBUG for more detail, TRACE for even more\n#log4j.logger.com.ibm.biginsights.bigsql.scheduler.Dev.Assignment=DEBUG,AssignStatInfo\n#log4j.appender.AssignStatInfo=org.apache.log4j.RollingFileAppender\n#log4j.appender.AssignStatInfo.file={{bigsql_log_dir}}/dev_pestats.log\n#log4j.appender.AssignStatInfo.layout=org.apache.log4j.PatternLayout\n#log4j.appender.AssignStatInfo.layout.ConversionPattern=%d{ISO8601} %p %c [%t] : %m%n\n#log4j.appender.AssignStatInfo.MaxFileSize=10MB\n#log4j.appender.AssignStatInfo.MaxBackupIndex=1\n\n# Uncomment and restart bigsql to get the details. Use INFO for less detail, DEBUG for more detail, TRACE for even more\n#log4j.logger.com.ibm.biginsights.bigsql.scheduler.Dev.PEStats=DEBUG,PEStatInfo\n#log4j.appender.PEStatInfo=org.apache.log4j.RollingFileAppender\n#log4j.appender.PEStatInfo.file={{bigsql_log_dir}}/dev_pestats.log\n#log4j.appender.PEStatInfo.layout=org.apache.log4j.PatternLayout\n#log4j.appender.PEStatInfo.layout.ConversionPattern=%d{ISO8601} %p %c [%t] : %m%n\n#log4j.appender.PEStatInfo.MaxFileSize=10MB\n#log4j.appender.PEStatInfo.MaxBackupIndex=1\",
          \"bigsql_log4j_content\" : \"\n# This file control logging for all Big SQL Java I/O and support processes\n# housed within FMP processes\nlog4j.rootLogger=WARN,verbose\n\nlog4j.appender.verbose=com.ibm.biginsights.bigsql.log.SharedRollingFileAppender\nlog4j.appender.verbose.file={{bigsql_log_dir}}/bigsql.log\nlog4j.appender.verbose.jniDirectory=/usr/ibmpacks/current/bigsql/bigsql/lib/native\nlog4j.appender.verbose.pollingInterval=30000\nlog4j.appender.verbose.layout=com.ibm.biginsights.bigsql.log.SharedServiceLayout\nlog4j.appender.verbose.layout.ConversionPattern=%d{ISO8601} %p %c [%t] : %m%n\nlog4j.appender.verbose.MaxFileSize={{bigsql_log_max_backup_size}}MB\nlog4j.appender.verbose.MaxBackupIndex={{bigsql_log_number_of_backup_files}}\n\n# Setting this to DEBUG will cause ALL queries to be traced, INFO will cause\n# only sessions that specifically request it to be traced\nlog4j.logger.bigsql.query.trace=INFO\n\n# Silence warnings about trying to override final parameters\nlog4j.logger.org.apache.hadoop.conf.Configuration=ERROR\n# Silence warnings about non existing hive properties\nlog4j.logger.org.apache.hadoop.hive.conf.HiveConf=ERROR\n\n# log4j.logger.com.ibm.biginsights.catalog=DEBUG\n# log4j.logger.com.ibm.biginsights.biga=DEBUG\",
          \"bigsql_log_max_backup_size\" : \"32\"
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     {
        \"type\":\"bigsql-logsearch-conf\",
	\"tag\":\"version1\", 
        \"properties_attributes\" : { },
        \"properties\" : {
          \"component_mappings\" : \"BIGSQL_HEAD:bigsql_server,bigsql_scheduler;BIGSQL_WORKER:bigsql_server\",
          \"content\" : \"\n{\n  \\"input\\":[\n    {\n     \\"type\\":\\"bigsql_server\\",\n     \\"rowtype\\":\\"service\\",\n     \\"path\\":\\"{{default('/configurations/bigsql-env/bigsql_log_dir', '/var/ibm/bigsql/logs')}}/bigsql.log\\"\n    },\n    {\n     \\"type\\":\\"bigsql_scheduler\\",\n     \\"rowtype\\":\\"service\\",\n     \\"path\\":\\"{{default('/configurations/bigsql-env/bigsql_log_dir', '/var/ibm/bigsql/logs')}}/bigsql-sched.log\\"\n    }\n  ],\n  \\"filter\\":[\n   {\n      \\"filter\\":\\"grok\\",\n      \\"conditions\\":{\n        \\"fields\\":{\n            \\"type\\":[\n                \\"bigsql_server\\",\n                \\"bigsql_scheduler\\"\n              ]\n            }\n      },\n      \\"log4j_format\\":\\"%d{ISO8601} %p %c [%t] : %m%n\\",\n      \\"multiline_pattern\\":\\"^(%{TIMESTAMP_ISO8601:logtime})\\",\n      \\"message_pattern\\":\\"(?m)^%{TIMESTAMP_ISO8601:logtime}%{SPACE}%{LOGLEVEL:level}%{SPACE}%{JAVACLASS:logger_name}%{SPACE}\\\\[%{DATA:thread_name}\\\\]%{SPACE}:%{SPACE}%{GREEDYDATA:log_message}\\",\n      \\"post_map_values\\":{\n        \\"logtime\\":{\n         \\"map_date\\":{\n          \\"target_date_pattern\\":\\"yyyy-MM-dd HH:mm:ss,SSS\\"\n         }\n       }\n     }\n    }\n   ]\n}\",
          \"service_name\" : \"IBM Big SQL\"
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X POST -d '
     {
        \"type\":\"bigsql-slider-env\",
	\"tag\":\"version1\", 	  
        \"properties_attributes\" : { },
        \"properties\" : {
          \"use_yarn_node_labels\" : \"false\",
          \"bigsql_yarn_label\" : \"bigsql\",
          \"bigsql_yarn_queue\" : \"default\",
          \"enforce_single_container\" : \"false\",
          \"bigsql_container_mem\" : \"28672\",
          \"bigsql_container_vcore\" : \"0\"
        }
     }
     ' http://localhost:8080/api/v1/clusters/$clusterName/configurations"""
	 
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-conf\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""
   
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-slider-flex\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""
   
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-head-env\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""
   
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-env\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""
   
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-users-env\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""
   
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-log4j\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""

   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-logsearch-conf\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""

   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Clusters\": {\"desired_configs\": { \"type\": \"bigsql-slider-env\", \"tag\" :\"version1\" }}}' http://localhost:8080/api/v1/clusters/$clusterName"""

   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)


   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -i -X POST -d '{\"host_components\" : [{\"HostRoles\":{\"component_name\":\"BIGSQL_HEAD\"}}] }' http://localhost:8080/api/v1/clusters/$clusterName/hosts?Hosts/host_name=$bigsqlHead"""
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(1 * 1000)

   for (workerNode <- Source.fromFile("/tmp/CreateClusterSetup/bigsqlWorkers").getLines) {
       command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -i -X POST -d '{\"host_components\" : [{\"HostRoles\":{\"component_name\":\"BIGSQL_WORKER\"}}] }' http://localhost:8080/api/v1/clusters/$clusterName/hosts?Hosts/host_name=$workerNode"""
       commandOut = executeCommands(Seq("bash", "-c", command)) 
       Thread.sleep(1 * 1000)
   }

   print("\n\n Installing BigSQL Service ")
   command = s"""curl -u admin:admin -H 'X-Requested-By: ambari' -i -X PUT -d '{\"ServiceInfo\": {\"state\" : \"INSTALLED\"}}'  http://localhost:8080/api/v1/clusters/$clusterName/services/BIGSQL"""
   commandOut = executeCommands(Seq("bash", "-c", command)) 
   Thread.sleep(30 * 1000)

  }


  private def setupSSH(nodes : ArrayBuffer[String]):Unit = {
   for ( node1 <- nodes ) {
    for ( node2 <- nodes ) {
       var command=s"""ssh $node1 "ssh-keyscan -t ecdsa $node2 >> /home/bigsql/.ssh/known_hosts""""
       var commandOut = executeCommands(Seq("bash", "-c", command))   

       command = s"""ssh $node1 "sshpass -p \\\"bigsql\\\" ssh-copy-id -i /home/bigsql/.ssh/id_rsa.pub -o StrictHostKeyChecking=no bigsql@$node2""""
       commandOut = executeCommands(Seq("bash", "-c", command))   
    }
   }
  

  }

  /** Setup the worker node **/
  private def loadWorkernodeCommands(nodeHostName : String):Unit = {
    workerCommands = List.empty
    var workerCommandsLB = ListBuffer[(String,String)]()

    workerCommandsLB += ("INSTALL_MYSQL" -> s"""ssh $nodeHostName "rpm -Uvh http://dev.mysql.com/get/mysql-community-release-el7-5.noarch.rpm"""")
    workerCommandsLB += ("INSTALL_PREREQ_RPM" -> s"""ssh $nodeHostName "yum install -y which ksh sudo lsof sshpass"""")

    workerCommandsLB += ("DELETE_TEMP_LOGS" -> s"""ssh $nodeHostName "rm -rf /tmp/bigsql* /tmp/db2*"""")
    workerCommandsLB += ("ADD_BIGSQL_USER" -> s"""ssh $nodeHostName "useradd -u 2824 -g hadoop bigsql"""")
    workerCommandsLB += ("CHANGE_BIGSQL_PASSWORD" -> s"""ssh $nodeHostName "echo bigsql:bigsql | chpasswd"""")
    workerCommandsLB += ("GENERATE_SSH_KEY" -> s"""ssh $nodeHostName "sudo -u bigsql ssh-keygen -f /home/bigsql/.ssh/id_rsa -t rsa -N ''"""")
    workerCommandsLB += ("SET_IPC_NO" -> "echo RemoveIPC=no >> /etc/systemd/logind.conf")

    workerCommands = workerCommandsLB.toList

  }

  /** Setup the head node **/
  private def loadHeadnodeCommands():Unit = {

    var headnodeCommandsLB = ListBuffer[(String,String)]()

    headnodeCommandsLB += ("INSTALL_MYSQL" -> "rpm -Uvh http://dev.mysql.com/get/mysql-community-release-el7-5.noarch.rpm")
    headnodeCommandsLB += ("INSTALL_PREREQ_RPM" -> "yum install -y which ksh sudo lsof sshpass")
    headnodeCommandsLB += ("INSTALL_BIGSQL_RPM" -> "rpm -ivh /tmp/CreateClusterSetup/bigsqlRPMs/IBM-Big_SQL-*.rpm")
    headnodeCommandsLB += ("ENABLE_BIGSQL" -> "/var/lib/ambari-server/resources/extensions/IBM-Big_SQL/*/scripts/EnableBigSQLExtension.py -U admin -P admin -NI")

    headnodeCommandsLB += ("DELETE_TEMP_LOGS" -> "rm -rf /tmp/bigsql* /tmp/db2*")
    headnodeCommandsLB += ("ADD_BIGSQL_USER" -> "useradd -u 2824 -g hadoop bigsql")
    headnodeCommandsLB += ("CHANGE_BIGSQL_PASSWORD" -> "echo bigsql:bigsql | chpasswd")
    headnodeCommandsLB += ("GENERATE_SSH_KEY" -> "sudo -u bigsql ssh-keygen -f /home/bigsql/.ssh/id_rsa -t rsa -N ''")
    headnodeCommandsLB += ("SET_IPC_NO" -> "echo RemoveIPC=no >> /etc/systemd/logind.conf")

    headnodeCommands = headnodeCommandsLB.toList

  }


  /** Setup the infrastructure **/
  private def loadinfraCommands():Unit = {
    var infraCommandsLB = ListBuffer[(String,String)]()
    infraCommandsLB += ("MKDIR_BIGSQL_RPM_FOLDER" -> "mkdir -p /tmp/CreateClusterSetup/bigsqlRPMs")
    infraCommandsLB += ("DOWNLOAD_BIGSQL_RPMS" -> s"wget --directory-prefix=/tmp/CreateClusterSetup/bigsqlRPMs $bigsqlRepoURL")
    infraCommands = infraCommandsLB.toList
  }


  /** Create Worker configuration **/
  private def createWorkerConfiguration(fileName : String, hostNames : Array[String]) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    hostNames.foreach(node => bw.write( node + "\n"))
    bw.close()
  }

  private def executeCommands(commandsList : List[(String, String)]) : Unit = {
    commandsList.foreach { case(commandDesc, value) => {
                  // println(s"\n\n\n\n Comm -> $commandDesc  \n\n\n Executing -> $value")
                  executeCommands(Seq("bash", "-c", value))
				
                }
            }
  }

  private def executeCommands(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    //println(s"\n\n\n\n Comm -> $cmd  \n $exitValue \n" + stdoutStream.toString +" \n"+  stderrStream.toString + "\n\n")
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }




}




