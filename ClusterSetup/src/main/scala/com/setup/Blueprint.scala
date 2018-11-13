package com.setup

import scala.util.control.Breaks.{breakable, break}
import scala.io.Source
import scala.sys.process._

import java.io.{InputStream, ByteArrayOutputStream, FileWriter, PrintWriter}

/**
  * Install the services using Ambari Blueprint
  */
class Blueprint(server: String, agentNodes: Array[String], clusterName: String) {

  /** Configure Blueprint and install in Ambari Server **/
  def install() {
    moveTemplate
    moveMapping
    print(s"\n\n Installing services using Ambari Blueprint")
    updateBlueprint
    print("\n\n Installing the Services. You can see the progress of the installation from Ambari UI")
    Thread.sleep(10 * 60 * 1000)
    checkServiceInstalledOrStarted
    recheckServiceStarted
    Process(Seq("bash", "-c", "rm -rf /tmp/CreateClusterSetup/tmp/*.*")).!
  }


  private def moveTemplate(){
    var fileStream : InputStream = null
    var out = Process(Seq("bash", "-c", "rm -rf /tmp/CreateClusterSetup/tmp/Blueprints.json")).!
    val fw = new FileWriter("/tmp/CreateClusterSetup/tmp/Blueprints.json", true)
    try {
      fileStream = getClass.getResourceAsStream("/Blueprints.json")
      val lines = Source.fromInputStream(fileStream).getLines
      lines.foreach(line => fw.write(line + "\n"))
    }
    finally {
      fw.close() 
      fileStream.close()
    }
  }

  private def moveMapping(){
    var fileStream : InputStream = null
    var out = Process(Seq("bash", "-c", "rm -rf /tmp/CreateClusterSetup/tmp/Hostmapping.json")).!
    val fw = new FileWriter("/tmp/CreateClusterSetup/tmp/Hostmapping.json", true)
    try {
      fileStream = getClass.getResourceAsStream("/Hostmapping.json")
      val lines = Source.fromInputStream(fileStream).getLines
      lines.foreach(line => fw.write(line + "\n"))
    }
    finally {
      fw.close() 
      fileStream.close()
    }
  }

  private def runCommand(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }


  private def updateBlueprint() {

    val serverDetails = s"""\\t{\\n\\t\\t\"name\" : \"host_group_1\",\\n\\t\\t\"hosts\" : [\\n\\t\\t\\t{\\n\\t\\t\\t\\t\"fqdn\" : \"$server\"\\n\\t\\t\\t}\\n\\t\\t]\\n\\t}\\n"""
        var command = s"""sed -i 's/ADD_NODE_1/$serverDetails/g'  /tmp/CreateClusterSetup/tmp/Hostmapping.json"""
    var out = runCommand(Seq("bash", "-c", command))

    if(agentNodes.length == 0){
      printf("TODO Throw ERROR")
    }else if(agentNodes.length == 1){
      val node = agentNodes(0)
      val nodeDetails = s"""\\t,{\\n\\t\\t\"name\" : \"host_group_2\",\\n\\t\\t\"hosts\" : [\\n\\t\\t\\t{\\n\\t\\t\\t\\t\"fqdn\" : \"$node\"\\n\\t\\t\\t}\\n\\t\\t]\\n\\t}\\n"""
            command = s"""sed -i 's/ADD_NODE_2/$nodeDetails/g'  /tmp/CreateClusterSetup/tmp/Hostmapping.json"""
             runCommand(Seq("bash", "-c", command))

      command = """sed -i '/ADD_NODE_3/d' /tmp/CreateClusterSetup/tmp/Hostmapping.json"""
            runCommand(Seq("bash", "-c", command))

    }else {

      val node = agentNodes(0)
      val nodeDetails = s"""\\t,{\\n\\t\\t\"name\" : \"host_group_2\",\\n\\t\\t\"hosts\" : [\\n\\t\\t\\t{\\n\\t\\t\\t\\t\"fqdn\" : \"$node\"\\n\\t\\t\\t}\\n\\t\\t]\\n\\t}\\n"""  
      command = s"""sed -i 's/ADD_NODE_2/$nodeDetails/g'  /tmp/CreateClusterSetup/tmp/Hostmapping.json"""
      runCommand(Seq("bash", "-c", command))

      var agentDetailsHead = s"""\\t,{\\n\\t\\t\"name\" : \"host_group_3\",\\n\\t\\t\"hosts\" : [\\n\\t\\t\\t"""
      var agentDetails = " "
      var agent = ""
      var startIndex = 1
      for(index <- 1 until agentNodes.length){
        
        agent=agentNodes(index)
        if(startIndex == 1)
          agentDetails = s"""$agentDetails{\\n\\t\\t\\t\\t\"fqdn\" : \"$agent\"\\n\\t\\t\\t}"""
        else
          agentDetails = s"""$agentDetails\\n\\t\\t\\t,{\\n\\t\\t\\t\\t\"fqdn\" : \"$agent\"\\n\\t\\t\\t}"""
        
        startIndex+=1  
        
      }
      var agentDetailsFooter = s"""\\n\\t\\t]\\n\\t}\\n"""
      agentDetails = s"""$agentDetailsHead$agentDetails$agentDetailsFooter"""

      command = s"""sed -i 's/ADD_NODE_3/$agentDetails/g'  /tmp/CreateClusterSetup/tmp/Hostmapping.json"""
      runCommand(Seq("bash", "-c", command))

    }
    
    command = s"""curl -H 'X-Requested-By:ambari' -X POST -u admin:admin http://localhost:8080/api/v1/blueprints/AmbariBlueprint -d @/tmp/CreateClusterSetup/tmp/Blueprints.json"""
    runCommand(Seq("bash", "-c", command))

    command = s"""curl -H 'X-Requested-By:ambari' -X POST -u admin:admin http://localhost:8080/api/v1/clusters/$clusterName -d @/tmp/CreateClusterSetup/tmp/Hostmapping.json"""
    runCommand(Seq("bash", "-c", command))

    Thread.sleep(2 * 60 * 1000)

  }


  private def checkServiceInstalledOrStarted() {

    var command = s"""curl --silent -u admin:admin -X GET http://localhost:8080/api/v1/clusters/$clusterName/services  | grep  service_name | sed -e 's,.*:.*"\\(.*\\)".*,\\1,g'"""
    var out = runCommand(Seq("bash", "-c", command))

    var services = out._2.split("\\r?\\n")
    var loopBreak = 30
    var index = 0
    for (service <- services){
      index = 0
      breakable {
        do {
          var comm = s"""curl -u admin:admin -H "X-Requested-By: ambari" -i -X GET http://localhost:8080/api/v1/clusters/$clusterName/services/$service  | grep \"INSTALLED\\|STARTED\""""
          var (exitValue, stdout, stderr) = runCommand(Seq("bash", "-c", comm))
          if((exitValue == 0 && ! stdout.isEmpty ) || index == loopBreak){
            break
          }
          Thread.sleep(2 * 60 * 1000)
          index += 1

        }while(true)
      }
    }
  }


  private def recheckServiceStarted() {

    var command = s"""curl --silent -u admin:admin -X GET http://localhost:8080/api/v1/clusters/$clusterName/services  | grep  service_name | sed -e 's,.*:.*"\\(.*\\)".*,\\1,g'"""
    var (exitValue, stdout, stderr) = runCommand(Seq("bash", "-c", command))

    var services = stdout.split("\\r?\\n")
    for (service <- services){
      breakable {

        var comm = s"""curl -u admin:admin -H "X-Requested-By: ambari" -i -X GET http://localhost:8080/api/v1/clusters/$clusterName/services/$service  | grep \"STARTED\""""

        var (exitValue, stdout, stderr) = runCommand(Seq("bash", "-c", comm))
        if(exitValue == 0 && ! stdout.isEmpty ){
          break
        }
        comm = s"""curl -u admin:admin -H "X-Requested-By: ambari" -i -X PUT -d '{"ServiceInfo": {"state" : "STARTED"}}'  http://localhost:8080/api/v1/clusters/$clusterName/services/$service"""
        runCommand(Seq("bash", "-c", comm))
        Thread.sleep(10 * 1000)

      }
    }
  }


}