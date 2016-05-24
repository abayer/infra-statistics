#!/usr/bin/env groovy
@Grab(group='com.gmongo', module='gmongo', version='0.9')
@Grab(group='org.codehaus.jackson', module='jackson-core-asl', version='1.9.3')
@Grab(group='org.codehaus.jackson', module='jackson-mapper-asl', version='1.9.3')
@Grab(group='org.postgresql', module='postgresql', version='9.3-1104-jdbc4')
@GrabConfig(systemClassLoader=true)
import com.gmongo.GMongo
import org.codehaus.jackson.*
import org.codehaus.jackson.map.*

import groovy.util.CliBuilder
import groovy.json.*

import java.sql.BatchUpdateException
import java.util.zip.*;
import groovy.sql.*


def parseArgs(cliArgs) {
    def cli = new CliBuilder(usage: "parse-usage.groovy [options]",
                             header: "Options")

    cli._(longOpt:'logs', args:1, required:true, "Directory containing raw logs")

    cli._(longOpt:'output', args:1, required:true, "Directory to output processed JSON to")

    cli._(longOpt:'timestamp', args:1, required:false, "Base timestamp for logs - i.e., '201112'")

    cli._(longOpt:'incremental', args:0, required:false, "Parse incrementally based on the available files in --logs and --output")

    cli._(longOpt:'mongoPort', args:1, required:false, "Port for Mongo to use - defaults to 27017")
    
    def options = cli.parse(cliArgs)

    assert new File(options.logs).isDirectory(), "--logs value ${options.logs} is not a directory"
    assert new File(options.output).isDirectory(), "--output value ${options.output} is not a directory"

    return options
}


def argResult = parseArgs(this.args)
def logDir=new File(argResult.logs)
def outputDir=new File(argResult.output);
def mongoPort = argResult.mongoPort ? Integer.valueOf(argResult.mongoPort) : 27017

byMonth=[:] as TreeMap
re = /.*log\.([0-9]{6})[0-9]+\.gz/
logDir.eachFileMatch(~re) { f ->
    m = (f=~re)
    if (m)  byMonth[m[0][1]] = true;
}
def data = byMonth.keySet() as List
println "Found logs: ${data}"

Sql.LOG.level = java.util.logging.Level.SEVERE
Sql db = Sql.newInstance("jdbc:postgresql://localhost:5432/usageDb", "stats", "admin", "org.postgresql.Driver")
createTablesIfNeeded(db)

// do not process the current month as the data may not be complete yet
data.pop()
data.each { String t ->
    process(db, t, logDir)
}

def getIDFromQuery(Sql db, String query) {
    def rows = db.rows(query)
    if (rows != null && !rows.isEmpty()) {
        return rows.first().get("id")
    }
    return null
}

def addRow(Sql db, String table, String field, String value) {
    return db.executeInsert("insert into ${table} (${field}) values ('${value}')".toString())[0][0]
}

def getInsertValuesString(Map<String,Object> fields) {
    return fields.values().collect {
        if (it == null) {
            return "null"
        } else {
            return "'${it}'"
        }
    }.join(',')
}

def getCopyValuesString(Map<String,Object> fields) {
    return fields.values().collect {
        if (it == null) {
            return "null"
        } else if (it instanceof Boolean) {
            return it
        } else {
            try {
                return Integer.valueOf(it)
            } catch (NumberFormatException e) {
                return "'${it}'".replaceAll(';',':')}
            }
        }
    }.join(';')
}

def getSelectValuesString(Map<String,Object> fields) {
    return fields.collect {
        if (it.value == null) {
            return "${it.key} is null"
        } else {
            return "${it.key} = '${it.value}'"
        }
    }.join(' AND ')
}

def addRow(Sql db, String table, Map<String,Object> fields) {
    return db.executeInsert("insert into ${table} (${fields.keySet().join(',')}) values (${getInsertValuesString(fields)})".toString())[0][0]
}

def batchRow(BatchingStatementWrapper stmt, String table, Map<String,Object> fields) {
    stmt.addBatch("insert into ${table} (${fields.keySet().join(',')}) values (${getInsertValuesString(fields)})".toString())
}

def copyRow(String table, Map<String,Object> fields) {
    return getCopyValuesString(fields)
}

def getRowId(Sql db, String table, String field, String value) {
    def id = getIDFromQuery(db, "select id from ${table} where ${field} = '${value}'")
    if (id == null) {
        id = addRow(db, table, field, value)
        //println "adding ${table} ${value} to id ${id}"
    }
    return id
}

def getRowId(Sql db, String table, Map<String,Object> fields) {
    def id = getIDFromQuery(db, "select id from ${table} where ${getSelectValuesString(fields)}")
    if (id == null) {
        id = addRow(db, table, fields)
        //println "adding ${table} ${fields} to id ${id}"
    }
    return id
}

def instanceRowId(Sql db, String instanceId, String container, String version, String dateString) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss")
    return addRow(db, "install", [identifier: instanceId, container: container, version: version,
        when_seen: whenSeen])
}

def addJobRecord(Integer installId, String jobType, Integer jobCount) {
    copyRow("job_record", [install_id: installId, job_type: jobType, job_count: jobCount])
    //println "adding job record for instance record ${instanceRecordId} and job type record ${jobTypeId}"
}

def addNodeRecord(Integer installId,
                  String jvmName, String jvmVersion, String jvmVendor,
                  String os, Boolean master, Integer executors) {
    copyRow("node_record", [install_id: installId, jvm_version: jvmVersion,
                                   jvm_vendor: jvmVendor, jvm_name: jvmName,
                                   os: os, master: master, executors: executors])
    //println "adding node record for instance record ${instanceRecordId} and some node"
}

def addPluginRecord(Integer installId, String plugin, String version) {
    copyRow("plugin_record", [install_id: installId, plugin: plugin, version: version])
    //println "adding plugin record for instance record ${instanceRecordId} and plugin version ${pluginVersionId}"
}

def failOrNot(Sql db, String query) {
    try {
        true
        //db.execute(query)
    } catch (Exception e) {
        //who cares
    }
}

def createTablesIfNeeded(Sql db) {
    db.execute("""CREATE TABLE IF NOT EXISTS install (
id SERIAL,
identifier varchar(64),
container varchar,
version varchar,
when_seen timestamp
);
""")

    failOrNot(db, "CREATE INDEX instance_identifier_idx ON instance (identifier);")

    db.execute("""CREATE TABLE IF NOT EXISTS job_record (
id SERIAL,
install_id integer,
job_type VARCHAR,
job_count integer
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS node_record (
id SERIAL,
install_id integer,
jvm_name varchar,
jvm_version varchar,
jvm_vendor varchar,
os varchar,
master boolean,
executors integer
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin_record (
id SERIAL,
install_id integer,
plugin varchar,
version varchar
);
""")

    db.execute("""
create table if not exists seen_logs (
filename varchar
)
""")

}

def process(Sql db, String timestamp, File logDir) {

    def procJson = [:]

    def ant = new AntBuilder()

    def slurper = new JsonSlurper()

    def tmpDir = new File("./tmp")

    if (!tmpDir.isDirectory()) {
        tmpDir.mkdirs()
    }

    def logRE = ".*log\\.${timestamp}.*gz"

    def linesSeen = 0
    def instColl = [:]
    def recCnt = 0
    def instanceIds = [:]
    def versionIds = [:]
    def containerIds = [:]
    def jvmIds = [:]
    def pluginIds = [:]
    def pluginVersionIds = [:]
    def jobTypeIds = [:]
    def osIds = [:]

    logDir.eachFileMatch(~/$logRE/) { origGzFile ->
        if (db.rows("select * from seen_logs where filename = ${origGzFile.name}").isEmpty()) {
            db.execute("insert into seen_logs values (${origGzFile.name})")

            println "Handing original log ${origGzFile.canonicalPath}"
            new GZIPInputStream(new FileInputStream(origGzFile)).eachLine("UTF-8") { l ->
                linesSeen++;
                def j = slurper.parseText(l)
                def installId = j.install
                def ver = j.version

                def jobCnt = j.jobs.values().inject(0) { acc, val -> acc + val }

                if (jobCnt > 0) {
                    if (!instColl.containsKey(installId)) {
                        instColl[installId] = []
                    }
                    instColl[installId] << j
                    recCnt++
                }
            }
        } else {
            println "Already saw ${origGzFile.name}, skipping"
        }
    }
    println "Filtering for multiple appearences..."
    def moreThanOne = instColl.findAll { it.value.size() > 2 }.values()
    println "Adding ${moreThanOne.size()} instances (${recCnt} records) for ${timestamp}"
    db.connection.autoCommit = false
    def nodesToCopy = []
    def pluginsToCopy = []
    def jobsToCopy = []
    moreThanOne.each { instList ->
        instList.each { j ->
            def installId = j.install
            def ver = j.version

            def recordId
            try {
                recordId = instanceRowId(db, installId, j.servletContainer, ver, j.timestamp)
            } catch (Exception e) {
                // do nothing
            }

            if (recordId != null) {
                j.nodes?.each { n ->
                    def isMaster = n.master ?: false
                    nodesToCopy << addNodeRecord(recordId, n.'jvm-name', n.'jvm-version', n.'jvm-vendor',
                        n.os, isMaster, n.executors)
                }

                j.plugins?.each { p ->
                    pluginsToCopy << addPluginRecord(recordId, p.name, p.version)
                }

                j.jobs?.each { type, cnt ->
                    jobsToCopy << addJobRecord(recordId, type, cnt)
                }
            }
        }
    }

    println "First commit"
    db.connection.commit()

    println "Writing temp files"
    def nodesTmpFile = File.createTempFile("nodes", timestamp)
    nodesTmpFile.write(nodesToCopy.join("\n"))

    def jobsTmpFile = File.createTempFile("jobs", timestamp)
    jobsTmpFile.write(jobsToCopy.join("\n"))

    def pluginsTmpFile = File.createTempFile("plugins", timestamp)
    pluginsTmpFile.write(pluginsToCopy.join("\n"))
    println "Batching up"
    db.connection.autoCommit = false
    try {
        db.withBatch { stmt ->
            stmt.addBatch("copy node_record(install_id, jvm_name, jvm_version, jvm_vendor, os, master, executors) from '${nodesTmpFile.canonicalPath}' delimiter ';' CSV")
            stmt.addBatch("copy job_record(install_id, job_type, job_count) from '${jobsTmpFile.canonicalPath}' delimiter ';' CSV")
            stmt.addBatch("copy plugin_record(install_id, plugin, version) from '${pluginsTmpFile.canonicalPath}' delimiter ';' CSV")
        }
    } catch (BatchUpdateException e) {
        throw e.getNextException()
    }

    println "Committing..."
    db.connection.commit()
}
