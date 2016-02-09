#!/usr/bin/env groovy
import groovyx.gpars.GParsPool

// push *.json.gz into a local SQLite database
import org.sqlite.*
import static groovyx.gpars.GParsPool.withPool

@Grapes([
        @Grab(group='org.codehaus.jackson', module='jackson-mapper-asl', version='1.9.13'),
        @Grab('org.xerial:sqlite-jdbc:3.7.2'),
        @Grab('org.codehaus.gpars:gpars:1.2.1'),
        @GrabConfig(systemClassLoader=true)
])


class NumberCollector {

    def db
    def workingDir

    def NumberCollector(workingDir, db){
        this.db = db
        this.workingDir = workingDir
    }

    def generateStats(File file) {

        if(!DBHelper.doImport(db, file.name)){
            println "skip $file - already imported..."
            return
        }

        def dateStr = file.name.substring(0, 6)
        def monthDate = java.util.Date.parse('yyyyMM', dateStr)
        int records=0;

        db.withTransaction({
            JenkinsMetricParser p = new JenkinsMetricParser()
            p.forEachInstance(file) { InstanceMetric metric ->
                if ((records++)%100==0)
                    System.out.print('.');
                def instId = metric.instanceId;

                db.execute("insert into jenkins(instanceid, month, version) values( $instId, $monthDate, ${metric.jenkinsVersion})")

                metric.plugins.each { pluginName, pluginVersion ->
                    db.execute("insert into plugin(instanceid, month, name, version) values( $instId, $monthDate, $pluginName, $pluginVersion)")
                }

                metric.jobTypes.each { jobtype, jobNumber ->
                    db.execute("insert into job(instanceid, month, type, jobnumber) values( $instId, $monthDate, $jobtype, $jobNumber)")
                }

                metric.nodesOnOs.each { os, nodesNumber ->
                    db.execute("insert into node(instanceid, month, osname, nodenumber) values( $instId, $monthDate, $os, $nodesNumber)")
                }

                db.execute("insert into executor(instanceid, month, numberofexecutors) values( $instId, $monthDate, $metric.totalExecutors)")
            }

            db.execute("insert into importedfile(name) values($file.name)")
        })

        println "\ncommited ${records} records for ${monthDate.format('yyyy-MM')}"
    }

    def run(String[] args) {
        if (args.length==0) {
            workingDir.eachFileMatch( ~".*json.gz" ) { file -> generateStats(file) }
        } else {
            args.each { name -> generateStats(new File(name)) }
        }
    }
}

File workingDir = new File("target")
def scratchDbs = [:]

workingDir.eachFileMatch( ~".*json.gz" ) { file ->
    def fileKey = file.name.replaceAll('.json.gz', '')
    println "fileKey: ${fileKey}"
    scratchDbs[fileKey] = DBHelper.setupDB(workingDir, "${fileKey}_stats.db")
}

withPool(5) {
    scratchDbs.eachParallel { dbKey, db ->
        println "Handling ${dbKey}"
        new NumberCollector(workingDir, db).run("${workingDir.absolutePath}/${dbKey}.json.gz")
    }
}

scratchDbs.each { dbKey, partialDb ->
    println "merging ${dbKey}..."
    DBHelper.mergeDbs(workingDir, "stats.db", "${dbKey}_stats.db")
}





