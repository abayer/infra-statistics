#!/usr/bin/env groovy
import groovyx.gpars.GParsPool

// push *.json.gz into a local SQLite database
import org.sqlite.*

@Grapes([
    @Grab(group='org.codehaus.jackson', module='jackson-mapper-asl', version='1.9.13'),
    @Grab('org.xerial:sqlite-jdbc:3.16.1'),
    @Grab('org.codehaus.gpars:gpars:1.2.0'),
    @Grab(group='mysql', module='mysql-connector-java', version='5.1.6'),
    @GrabConfig(systemClassLoader=true)
])


class NumberCollector {

    def workingDir

    def NumberCollector(workingDir){
        this.workingDir = workingDir
    }

    def generateStats(File file) {
        def db = DBHelper.setupDB(workingDir)
        if(!DBHelper.doImport(db, file.name)){
            println "skip $file - already imported..."
            return
        }
        long start = System.currentTimeMillis()
        def dateStr = file.name.substring(0, 6)
        def monthDate = java.util.Date.parse('yyyyMM', dateStr)
        def monthStr = monthDate.format("yyyy-MM-dd")
        int records=0;

        db.execute("insert into startedfiles(name, started) values(?, now())", [file.name])
        JenkinsMetricParser p = new JenkinsMetricParser()
        db.withTransaction {
            p.forEachInstance(file) { InstanceMetric metric ->
                def instId = metric.instanceId.substring(0, 64);
                def tableId = db.executeInsert("insert into instance_month(instanceid, month) values (?,?)",
                    [instId, monthStr]).first().first()
                records++

                db.executeInsert("insert into jenkins(instid, version, jvmvendor, jvmname, jvmversion, numberofexecutors) values(?,?,?,?,?,?)",
                    [tableId, metric.jenkinsVersion, metric.jvm?.vendor, metric.jvm?.name, metric.jvm?.version, metric.totalExecutors])

                metric.plugins.each { pluginName, pluginVersion ->
                    db.executeInsert("insert into plugin(instid, name, version) values(?,?,?)",
                        [tableId, pluginName, pluginVersion])
                }

                metric.jobTypes.each { jobtype, jobNumber ->
                    db.executeInsert("insert into job(instid, type, jobnumber) values(?,?,?)",
                        [tableId, jobtype, jobNumber])
                }

                metric.nodesOnOs.each { os, nodesNumber ->
                    db.executeInsert("insert into node(instid, osname, nodenumber) values(?,?,?)",
                        [tableId, os.toString(), nodesNumber])
                }

            }
        }
        db.execute("update startedfiles set finished = now() where name = ?", [file.name])

        println "\ncommited ${records} records in ${((System.currentTimeMillis() - start) / 1000).round(0)} seconds for ${monthDate.format('yyyy-MM')}"
    }

    def run(String[] args) {
        if (args.length==0) {
            workingDir.eachFileMatch( ~".*json.gz" ) { file -> generateStats(file) }
        } else {
            GParsPool.withPool(20) {
                args.eachParallel { name -> generateStats(new File(name)) }
            }
        }
    }
}

def workingDir = new File("target")
new NumberCollector(workingDir).run(args)




