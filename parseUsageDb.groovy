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

def trackedIds = [instanceIds: [:],
                  versionIds: [:],
                  containerIds: [:],
                  jvmIds: [:],
                  pluginIds: [:],
                  pluginVersionIds: [:],
                  jobTypeIds: [:],
                  osIds: [:]]


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
//    println "trackedIds.instanceIds size : ${trackedIds['instanceIds'].size()}"
}

def getIDFromQuery(Sql db, String query) {
    def rows = db.rows(query)
    if (rows != null && !rows.isEmpty()) {
        return rows.first().get("id")
    }
    return null
}

def addUniqueRow(BatchingStatementWrapper stmt, String table, Map<String,Object> fields) {
    String query = "insert into ${table} (${fields.keySet().join(',')}) select ${getInsertValuesString(fields)} " +
        "where not exists (select id from ${table} where ${getSelectValuesString(fields)})"
//    println "q: ${query}"
    stmt.addBatch(query)
}

def addRow(BatchingStatementWrapper stmt, String table, String field, String value) {
    String query = "insert into ${table} (${field}) values ('${value}')"
    //println "q: ${query}"
    stmt.addBatch(query)
}

def getInsertValuesString(Map<String,Object> fields) {
    return fields.values().collect {
        if (it == null) {
            return "null"
        } else if ((it instanceof GString || it instanceof String) && it.startsWith("select ")) {
            return "(${it})"
        } else {
            try {
                return Integer.valueOf(it)
            } catch (NumberFormatException e) {
                return "'${it}'"
            }
        }
    }.join(',')
}

def getSelectValuesString(Map<String,Object> fields) {
    return fields.collect {
        if (it.value == null) {
            return "${it.key} is null"
        } else if ((it.value instanceof GString || it.value instanceof String) && it.value.startsWith("select ")) {
            return "${it.key} = (${it.value})"
        } else {
            try {
                return "${it.key} = ${Integer.valueOf(it.value)}"
            } catch (NumberFormatException e) {
                return "${it.key} = '${it.value}'"
            }
        }
    }.join(' AND ')
}

def addRow(BatchingStatementWrapper stmt, String table, Map<String,Object> fields) {
    String query = "insert into ${table} (${fields.keySet().join(',')}) values (${getInsertValuesString(fields)})"
    //println "q: ${query}"
    stmt.addBatch(query)
}

def getRowId(BatchingStatementWrapper stmt, String table, String field, String value) {
    return addRow(stmt, table, field, value)
}

def getRowId(BatchingStatementWrapper stmt, String table, Map<String,Object> fields) {
    return addRow(stmt, table, fields)
}

def instanceRowId(BatchingStatementWrapper stmt, String instanceId) {
    return addUniqueRow(stmt, "instance", [identifier: instanceId])
}

def jenkinsVersionRowId(BatchingStatementWrapper stmt, String versionString) {
    return addUniqueRow(stmt, "jenkins_version", [version_string: versionString])
}

def containerRowId(BatchingStatementWrapper stmt, String containerString) {
    return addUniqueRow(stmt, "servlet_container", [container_name: containerString])
}

def jobTypeRowId(BatchingStatementWrapper stmt, String className) {
    return addUniqueRow(stmt, "job_type", [class_name: className])
}

def jvmRowId(BatchingStatementWrapper stmt, String jvmName, String jvmVersion, String jvmVendor) {
    return addUniqueRow(stmt, "jvm", [jvm_name: jvmName, jvm_version: jvmVersion, jvm_vendor: jvmVendor])
}

def osRowId(BatchingStatementWrapper stmt, String osName) {
    return addUniqueRow(stmt, "os", [os_name: osName])
}

def pluginRowId(BatchingStatementWrapper stmt, String pluginName) {
    return addUniqueRow(stmt, "plugin", [plugin_name: pluginName])
}

def pluginVersionRowId(BatchingStatementWrapper stmt, String versionString, String pluginName) {
    return addUniqueRow(stmt, "plugin_version", [plugin_id: "select id from plugin where plugin_name = '${pluginName}'",
                                             version_string: versionString])
}

def addInstanceRecord(BatchingStatementWrapper stmt, String identifier, String containerName, String jenkinsVersion, String dateString) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss")

//    def existingRow = getIDFromQuery(db, "select id from instance_record where instance_id = ${instanceId} and when_seen = '${whenSeen}'")
//    if (existingRow == null) {
    addRow(stmt, "instance_record", [instance_id: "select id from instance where identifier = '${identifier}'",
                                     servlet_container_id: "select id from servlet_container where container_name = '${containerName}'",
                                     jenkins_version_id: "select id from jenkins_version where version_string = '${jenkinsVersion}'",
                                     when_seen: whenSeen])
/*    } else {
        return existingRow
    }*/
}

def addJobRecord(BatchingStatementWrapper stmt, String identifier, String dateString, String jobType, Integer jobCount) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss")
//    def existingRow = getIDFromQuery(db, "select id from job_record where instance_record_id = ${instanceRecordId} and job_type_id = '${jobTypeId}'")
//    if (existingRow == null) {
    addRow(stmt, "job_record", [instance_record_id: "select id from instance_record where instance_id = (select id from instance where identifier = '${identifier}') and when_seen = '${whenSeen}'",
                                job_type_id: "select id from job_type where class_name = '${jobType}'",
                                job_count: jobCount])

    //println "adding job record for instance record ${instanceRecordId} and job type record ${jobTypeId}"
}

def addNodeRecord(BatchingStatementWrapper stmt, String identifier, String dateString,
                  String jvmName, String jvmVersion, String jvmVendor, String osName, Boolean master, Integer executors) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss")
    addRow(stmt, "node_record", [instance_record_id: "select id from instance_record where instance_id = (select id from instance where identifier = '${identifier}') and when_seen = '${whenSeen}'",
                                 jvm_id: "select id from jvm where jvm_name = '${jvmName}' and jvm_version = '${jvmVersion}' and jvm_vendor = '${jvmVendor}'",
                                 os_id: "select id from os where os_name = '${osName}'",
                                 master: master, executors: executors])
    //println "adding node record for instance record ${instanceRecordId} and some node"
}

def addPluginRecord(BatchingStatementWrapper stmt, String identifier, String dateString, String pluginName, String pluginVersion) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss")

//    if (existingRow == null) {
    addRow(stmt, "plugin_record", [instance_record_id: "select id from instance_record where instance_id = (select id from instance where identifier = '${identifier}') and when_seen = '${whenSeen}'",
                                   plugin_version_id: "select id from plugin_version where plugin_id = (select id from plugin where plugin_name = '${pluginName}') and version_string = '${pluginVersion}'"])
//    }
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
    db.execute("""CREATE TABLE IF NOT EXISTS instance (
id SERIAL PRIMARY KEY,
identifier varchar(64),
CONSTRAINT unique_id UNIQUE(identifier)
);
""")

    failOrNot(db, "CREATE INDEX instance_identifier_idx ON instance (identifier);")

    db.execute("""CREATE TABLE IF NOT EXISTS servlet_container (
id SERIAL PRIMARY KEY,
container_name varchar,
CONSTRAINT unique_container UNIQUE(container_name)
);
""")

    failOrNot(db, "CREATE INDEX container_name_idx ON servlet_container (container_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS jenkins_version (
id SERIAL PRIMARY KEY,
version_string varchar,
CONSTRAINT unique_version UNIQUE(version_string)
);
""")

    failOrNot(db, "CREATE INDEX jenkins_version_idx ON jenkins_version (version_string);")

    db.execute("""CREATE TABLE IF NOT EXISTS job_type (
id SERIAL PRIMARY KEY,
class_name varchar,
CONSTRAINT unique_type UNIQUE(class_name)
);
""")

    failOrNot(db, "CREATE INDEX job_type_idx ON job_type (class_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS jvm (
id SERIAL PRIMARY KEY,
jvm_name varchar,
jvm_version varchar,
jvm_vendor varchar,
CONSTRAINT unique_jvm UNIQUE(jvm_name, jvm_version, jvm_vendor)
);
""")

    failOrNot(db, "CREATE INDEX jvm_idx ON jvm (jvm_name, jvm_version, jvm_vendor);")

    db.execute("""CREATE TABLE IF NOT EXISTS os (
id SERIAL PRIMARY KEY,
os_name varchar,
CONSTRAINT unique_os UNIQUE(os_name)
);
""")

    failOrNot(db, "CREATE INDEX os_idx ON os (os_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin (
id SERIAL PRIMARY KEY,
plugin_name varchar,
CONSTRAINT unique_plugin UNIQUE(plugin_name)
);
""")

    failOrNot(db, "CREATE INDEX plugin_idx ON plugin (plugin_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin_version (
id SERIAL PRIMARY KEY,
plugin_id integer ,
version_string varchar,
CONSTRAINT unique_plugin_version UNIQUE(plugin_id, version_string)
);
""")

    failOrNot(db, "CREATE INDEX plugin_version_idx ON plugin_version (plugin_id, version_string);")

    db.execute("""CREATE TABLE IF NOT EXISTS instance_record (
id SERIAL PRIMARY KEY,
instance_id integer,
servlet_container_id integer,
jenkins_version_id integer,
when_seen TIMESTAMP,
CONSTRAINT unique_instance_record UNIQUE(instance_id, when_seen)
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS job_record (
id SERIAL PRIMARY KEY,
instance_record_id integer,
job_type_id integer,
job_count integer,
CONSTRAINT unique_job_record UNIQUE(instance_record_id, job_type_id)
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS node_record (
id SERIAL PRIMARY KEY,
instance_record_id integer,
jvm_id integer,
os_id integer,
master boolean,
executors integer
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin_record (
id SERIAL PRIMARY KEY,
instance_record_id integer,
plugin_version_id integer,
CONSTRAINT unique_plugin_record UNIQUE(instance_record_id, plugin_version_id)
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
    def alreadySeenPlugins = [:]
    def alreadySeenInstances = [:]
    def alreadySeenJobs = [:]
    db.connection.autoCommit = false
    def moreThanOne = instColl.findAll { it.value.size() > 2 }.values()
    println "Adding ${moreThanOne.size()} instances (${recCnt} records) for ${timestamp}"
    try {
        db.withBatch { stmt ->
            moreThanOne.each { instList ->
                def j = instList.sort { a, b -> Date.parse("dd/MMM/yyyy:H:m:s Z", a.timestamp) <=> Date.parse("dd/MMM/yyyy:H:m:s Z", b.timestamp) }.last()

                def installId = j.install
                def ver = j.version

                instanceRowId(stmt, installId)


                jenkinsVersionRowId(stmt, ver)

                containerRowId(stmt, j.servletContainer)
                if (!alreadySeenInstances.containsKey(installId)) {
                    addInstanceRecord(stmt, installId, j.servletContainer, ver, j.timestamp)
                    alreadySeenInstances[installId] = true
                }

                j.nodes?.each { n ->
                    if (n."jvm-name" != null && n."jvm-version" != null && n."jvm-vendor" != null) {
                        jvmRowId(stmt, n."jvm-name", n."jvm-version", n."jvm-vendor")
                    }
                    def isMaster = n.master ?: false
                    osRowId(stmt, n.os)
                    def executors = n.executors

                    addNodeRecord(stmt, installId, j.timestamp, n."jvm-name", n.'jvm-version', n.'jvm-vendor',
                        n.os, isMaster, executors)

                }

                j.plugins?.each { p ->
                    pluginRowId(stmt, p.name)

                    pluginVersionRowId(stmt, p.version, p.name)

                    if (!alreadySeenPlugins.containsKey([installId, p.name, p.version])) {
                        addPluginRecord(stmt, installId, j.timestamp, p.name, p.version)
                        alreadySeenPlugins[[installId, p.name, p.version]] = true
                    }
                }

                j.jobs?.each { type, cnt ->
                    jobTypeRowId(stmt, type)

                    if (!alreadySeenJobs.containsKey([installId, type])) {
                        addJobRecord(stmt, installId, j.timestamp, type, cnt)
                        alreadySeenJobs[[installId, type]] = true
                    }
                }

            }
        }
    } catch (BatchUpdateException e) {
        throw e.getNextException()
    }
    db.connection.commit()
}
