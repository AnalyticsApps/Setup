package com.setup

import scala.io.{Source, StdIn}
import scala.sys.process._

object Setup {

  def main(args: Array[String]): Unit = setupCluster(args)
  
  private def setupCluster(args: Array[String]){
    println("\n\n Setting up the Ambari Server & Agents & Services (Estimated time for completion is 30 mins) ")
    Thread.sleep(1 * 1000)
    try{

      if (args.length == 0) {
          print("\n\n URL for ambari.repo file : ")
          val ambariRepoURL = scala.io.StdIn.readLine()
          //val ambariRepoURL= "http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.2.0/ambari.repo"

          print("\n\n Hostname for the nodes in the cluster (more than one hostname delimited by comma): ")
          val hostNames = StdIn.readLine()

          print("\n\n Cluster Name : ")
          val clusterName = StdIn.readLine()

          val setup = new SetupAmbari(ambariRepoURL, hostNames)
          setup.process()

          val bp = new Blueprint("hostname -f".!!.trim, Source.fromFile("/tmp/CreateClusterSetup/agentNodes").getLines.toArray, clusterName)
          bp.install()
         }else {
          val ambariRepoURL = args(0).trim
          val hostNames = args(1).trim
          val clusterName = args(2).trim

          val setup = new SetupAmbari(ambariRepoURL, hostNames)
          setup.process()

          val bp = new Blueprint("hostname -f".!!.trim, Source.fromFile("/tmp/CreateClusterSetup/agentNodes").getLines.toArray, clusterName)
          bp.install()
         }
       } finally {
         "rm -rf /tmp/CreateClusterSetup/tmp/*.*".!!
       }

    print("\n\n Setting up of Ambari Server & Agents & Services completed !!!\n\n")

    Thread.sleep(10 * 1000)

  }
}
