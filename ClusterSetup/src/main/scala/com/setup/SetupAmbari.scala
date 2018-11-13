package com.setup

import scala.sys.process._
import scala.io.{Source, StdIn}
import java.io.{File, FileWriter, BufferedWriter}
import scala.collection.mutable.ListBuffer

import java.io.{InputStream, ByteArrayOutputStream, FileWriter, PrintWriter}

/**
  * Setup the infrastructure and download the Ambari RPMs from public repository
  * Install the Ambari Server and Agents in the cluster.
  */
class SetupAmbari(ambariRepoURL: String, hostNames: String) {

  var infraSNCommands : List[(String, String)] = List.empty
  var infraANCommands : List[(String, String)] = List.empty
  var ambariSNCommands : List[(String, String)] = List.empty
  var ambariANCommands : List[(String, String)] = List.empty

  /** Setup the Ambari Server and Agent **/
  def process(): Unit = {
    processServerNode()
    processAgentNodes()

  }


  /** Setup the infrastructure on sever node **/
  private def loadinfraSNCommands():Unit = {

    var infraSNCommandsLB = ListBuffer[(String,String)]()

    infraSNCommandsLB += ("RM_SETUP_FOLDER" -> "rm -rf /tmp/CreateClusterSetup")
    infraSNCommandsLB += ("MKDIR_SETUP_FOLDER" -> "mkdir -p /tmp/CreateClusterSetup")
    infraSNCommandsLB += ("MKDIR_RPM_FOLDER" -> "mkdir -p /tmp/CreateClusterSetup/RPMs")
    infraSNCommandsLB += ("MKDIR_TEMP_FOLDER" -> "mkdir -p /tmp/CreateClusterSetup/tmp")
    infraSNCommandsLB += ("STATUS_1" -> "echo \n\nDownloading Ambari RPMs\n")
    infraSNCommandsLB += ("AMBARI_REPO_URL" -> s"wget $ambariRepoURL -O /tmp/CreateClusterSetup/ambari.repo")
    infraSNCommandsLB += ("COPY_AMBARI_REPO" -> "cp /tmp/CreateClusterSetup/ambari.repo /etc/yum.repos.d")
    infraSNCommandsLB += ("YUM_CLEAN_ALL" -> "yum clean all")
    infraSNCommandsLB += ("DOWNLOAD_AMBARI_RPMS" -> "yum install --downloadonly --downloaddir=/tmp/CreateClusterSetup/RPMs/ ambari-server ambari-agent")
    infraSNCommandsLB += ("CHANGE_PERMISSION_FOLDER" -> "chmod -R 777 /tmp/CreateClusterSetup")

    infraSNCommands = infraSNCommandsLB.toList

  }

  /** Setup the infrastructure on agent nodes in the cluster **/
  private def loadinfraANCommands(nodeHostName : String):Unit = {

    infraANCommands = List.empty
    var infraANCommandsLB = ListBuffer[(String,String)]()

    infraANCommandsLB += ("RM_SETUP_FOLDER" -> s"ssh $nodeHostName rm -rf /tmp/CreateClusterSetup")
    infraANCommandsLB += ("MKDIR_SETUP_FOLDER" -> s"ssh $nodeHostName mkdir -p /tmp/CreateClusterSetup")
    infraANCommandsLB += ("MKDIR_RPM_FOLDER" -> s"ssh $nodeHostName mkdir -p /tmp/CreateClusterSetup/RPMs")
    infraANCommandsLB += ("AMBARI_REPO_URL" -> s"scp /tmp/CreateClusterSetup/ambari.repo root@$nodeHostName:/tmp/CreateClusterSetup/")
    infraANCommandsLB += ("COPY_AMBARI_REPO" -> s"ssh $nodeHostName cp /tmp/CreateClusterSetup/ambari.repo /etc/yum.repos.d")
    infraANCommandsLB += ("YUM_CLEAN_ALL" -> s"ssh $nodeHostName yum clean all")
    infraANCommandsLB += ("DOWNLOAD_AMBARI_RPMS" -> s"scp /tmp/CreateClusterSetup/RPMs/ambari-agent*.rpm root@$nodeHostName:/tmp/CreateClusterSetup/RPMs/")
    infraANCommandsLB += ("CHANGE_PERMISSION_FOLDER" -> s"ssh $nodeHostName chmod -R 777 /tmp/CreateClusterSetup")

    infraANCommands = infraANCommandsLB.toList
  }

  /** Setup the ambari server **/
  private def loadambariSNCommands(ambariServerHostName : String):Unit = {

    var ambariSNCommandsLB = ListBuffer[(String, String)]()

    ambariSNCommandsLB += ("PREREQ_ENABLE_NTPD" -> "systemctl enable ntpd.service")
    ambariSNCommandsLB += ("PREREQ_START_NTPD" -> "systemctl start ntpd.service")
    ambariSNCommandsLB += ("PREREQ_INCREASE_LIMIT" -> "sed -i '$ i\\* - nofile 65536' /etc/security/limits.conf")
    ambariSNCommandsLB += ("PREREQ_INCREASE_LIMIT" -> "sed -i '$ i\\* - nproc 65536' /etc/security/limits.conf")

    ambariSNCommandsLB += ("PREREQ_TIMEZONE" -> "timedatectl set-timezone America/New_York")
    ambariSNCommandsLB += ("PREREQ_DISABLE_HUGESPACE" -> "echo never > /sys/kernel/mm/transparent_hugepage/enabled")
    ambariSNCommandsLB += ("PREREQ_DISABLE_HUGESPACE" -> s"""echo -e "if test -f /sys/kernel/mm/transparent_hugepage/enabled; then \n                                                                    							            #    echo never > /sys/kernel/mm/transparent_hugepage/enabled \nfi" >> /etc/rc.local""".stripMargin('#'))
    ambariSNCommandsLB += ("INSTALL_AMBARI_RPM" -> "yum -y install /tmp/CreateClusterSetup/RPMs/ambari-server-*.rpm /tmp/CreateClusterSetup/RPMs/ambari-agent-*.rpm")
    ambariSNCommandsLB += ("AMBARI_SERVER_SETUP" -> "ambari-server setup -s")
    ambariSNCommandsLB += ("AMBARI_SERVER_START" -> "ambari-server start")
    ambariSNCommandsLB += ("AMBARI_SERVER_STOP" -> "ambari-server stop")
    ambariSNCommandsLB += ("AMBARI_SERVER_RESET" -> "ambari-server reset -s")
    ambariSNCommandsLB += ("AMBARI_SERVER_RESTART" -> "ambari-server start")
    ambariSNCommandsLB += ("INSTALL_MYSQL_CONNECTOR" -> "yum -y install mysql-connector-java")
    ambariSNCommandsLB += ("CONFIGURE_MYSQL_WITH_AMBARI" -> "ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar")
    ambariSNCommandsLB += ("UPDATE_AMBARI_AGENT_INI" -> s"sed -i 's/hostname=localhost/hostname=$ambariServerHostName/g' /etc/ambari-agent/conf/ambari-agent.ini")
    ambariSNCommandsLB += ("UPDATE_AMBARI_AGENT_INI_TLS" -> "echo $'\n\n[security]\nforce_https_protocol=PROTOCOL_TLSv1_2\n' >> /etc/ambari-agent/conf/ambari-agent.ini")
    ambariSNCommandsLB += ("AMBARI_AGENT_START" -> "ambari-agent start")

    ambariSNCommands = ambariSNCommandsLB.toList

  }

  /** Setup the ambari agents in all the nodes **/
  private def loadambariANCommands(ambariServerHostName : String, nodeHostName : String):Unit = {

    ambariANCommands = List.empty
    var ambariANCommandsLB = ListBuffer[(String, String)]()

    ambariANCommandsLB += ("PREREQ_INCREASE_LIMIT" -> s"""ssh $nodeHostName \"sed -i '$$ i\\\\* - nofile 65536' /etc/security/limits.conf\"""")
    ambariANCommandsLB += ("PREREQ_INCREASE_LIMIT" -> s"""ssh $nodeHostName \"sed -i '$$ i\\\\* - noproc 65536' /etc/security/limits.conf\"""")

    ambariANCommandsLB += ("PREREQ_ENABLE_NTPD" -> s"ssh $nodeHostName systemctl enable ntpd.service")
    ambariANCommandsLB += ("PREREQ_START_NTPD" -> s"ssh $nodeHostName systemctl start ntpd.service")
    ambariANCommandsLB += ("PREREQ_TIMEZONE" -> s"ssh $nodeHostName timedatectl set-timezone America/New_York")
    ambariANCommandsLB += ("PREREQ_DISABLE_HUGESPACE" -> s"ssh $nodeHostName echo never > /sys/kernel/mm/transparent_hugepage/enabled")
    ambariANCommandsLB += ("PREREQ_DISABLE_HUGESPACE" -> s"""ssh $nodeHostName \"echo -e \\\"if test -f /sys/kernel/mm/transparent_hugepage/enabled; then \n                        										   #    echo never > /sys/kernel/mm/transparent_hugepage/enabled\nfi\\\" >> /etc/rc.local\"""".
						                             stripMargin('#'))
    ambariANCommandsLB += ("INSTALL_AMBARI_RPM" -> s"ssh $nodeHostName yum -y install /tmp/CreateClusterSetup/RPMs/ambari-agent-*.rpm")
    ambariANCommandsLB += ("UPDATE_AMBARI_AGENT_INI" -> s"""ssh $nodeHostName \"sed -i 's/hostname=localhost/hostname=$ambariServerHostName/g' /etc/ambari-agent/conf/ambari-agent.ini\"""")
    ambariANCommandsLB += ("UPDATE_AMBARI_AGENT_INI_TLS" -> s"""ssh $nodeHostName \"echo $$'\n\n[security]\nforce_https_protocol=PROTOCOL_TLSv1_2\n' >> /etc/ambari-agent/conf/ambari-agent.ini\"""")
    ambariANCommandsLB += ("AMBARI_AGENT_START" -> s"ssh $nodeHostName ambari-agent start")

    ambariANCommands = ambariANCommandsLB.toList
  }


  /** Create Node configuration **/
  private def createNodesConfiguration(fileName : String, hostNames : Array[String]) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    hostNames.foreach(node => bw.write( node + "\n"))
    bw.close()
  }

  private def executeCommands(commandsList : List[(String, String)]) : Unit = {
    commandsList.foreach { case(commandDesc, value) => {
                   //println(s"\n\n Comm -> $commandDesc  \n Executing -> $value")
                   //var out = Process(Seq("bash", "-c", value)).!
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
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }

  private def processServerNode(): Unit = {

    print("\n\n Downloding Ambari RPMs")
    val node = "hostname -f".!!.trim
    var out = executeCommands(Seq("bash", "-c", s"""grep '$node' /etc/hosts | awk '{print $$1}'"""))
    var ip = out._2.trim
    out = executeCommands(Seq("bash", "-c", s"""ssh-keyscan -t ecdsa $node,$ip >> /root/.ssh/known_hosts"""))

    loadinfraSNCommands()
    executeCommands(infraSNCommands)
    createNodesConfiguration("/tmp/CreateClusterSetup/agentNodes", hostNames.split(",").map(_.trim()))
    print("\n\n Setting up the Ambari Server")

    loadambariSNCommands("hostname -f".!!.trim)
    executeCommands(ambariSNCommands)
  }

  private def processAgentNodes(): Unit = {
    val filename = "/tmp/CreateClusterSetup/agentNodes"
    for (node <- Source.fromFile(filename).getLines) {
        print(s"\n\n Installing the Ambari Agent on -> $node")
         
        var out = executeCommands(Seq("bash", "-c", s"""grep '$node' /etc/hosts | awk '{print $$1}'"""))
        var ip = out._2.trim
        out = executeCommands(Seq("bash", "-c", s"""ssh-keyscan -t ecdsa $node,$ip >> /root/.ssh/known_hosts"""))

        loadinfraANCommands(node)
        executeCommands(infraANCommands)

        print(s"\n\n Configuring the Ambari Agent on -> $node")
        loadambariANCommands("hostname -f".!!.trim, node)
        executeCommands(ambariANCommands)

    }
  }


}

