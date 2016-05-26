#!/usr/bin/env groovy
@Grab(group='com.gmongo', module='gmongo', version='0.9')
@Grab(group='org.codehaus.jackson', module='jackson-core-asl', version='1.9.3')
@Grab(group='org.codehaus.jackson', module='jackson-mapper-asl', version='1.9.3')
@Grab(group='org.postgresql', module='postgresql', version='9.3-1104-jdbc4')
@Grab(group='org.postgresql', module='postgresql', version='9.3-1104-jdbc4')
@Grab(group='org.codehaus.gpars', module='gpars', version='1.2.1')

@GrabConfig(systemClassLoader=true)
import com.gmongo.GMongo
import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import static groovyx.gpars.GParsPool.withPool

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

    cli._(longOpt:'incremental', args:0, required:false,
        "Parse incrementally based on the available files in --logs and --output")

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
        process(t, logDir)
//    println "trackedIds.instanceIds size : ${trackedIds['instanceIds'].size()}"
}

def getIDFromQuery(Sql db, String table, Map<String,Object> fields) {
    String query = "select id from ${table} where ${getSelectValuesString(fields)}"
//    println "q: ${query}"
    def rows = db.rows(query)
    if (rows != null && !rows.isEmpty()) {
        return rows.first().get("id")
    }
    return null
}

def addUniqueRow(Sql db, String table, Map<String,Object> fields) {
    def id = getIDFromQuery(db, table, fields)
    if (id == null) {
        String query = "insert into ${table} (${fields.keySet().join(',')}) values (${getInsertValuesString(fields)})"
        db.execute(query)
        id = getIDFromQuery(db, table, fields)
    }

    return id
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
            } catch (Exception e) {
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
        } else if (it.value instanceof String && it.key.contains("version")) {
            return "${it.key} = '${it.value}'"
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

def instanceRowId(Sql db, String instanceId) {
    return addUniqueRow(db, "instance", [identifier: instanceId])
}

def jenkinsVersionRowId(Sql db, String versionString) {
    return addUniqueRow(db, "jenkins_version", [version_string: versionString])
}

def containerRowId(Sql db, String containerString) {
    return addUniqueRow(db, "servlet_container", [container_name: containerString])
}

def jobTypeRowId(Sql db, String className) {
    return addUniqueRow(db, "job_type", [class_name: className])
}

def jvmRowId(Sql db, String jvmName, String jvmVersion, String jvmVendor) {
    return addUniqueRow(db, "jvm", [jvm_name: jvmName, jvm_version: jvmVersion, jvm_vendor: jvmVendor])
}

def osRowId(Sql db, String osName) {
    return addUniqueRow(db, "os", [os_name: osName])
}

def pluginRowId(Sql db, String pluginName) {
    return addUniqueRow(db, "plugin", [plugin_name: pluginName])
}

def pluginVersionRowId(Sql db, String versionString, Integer pluginId) {
    return addUniqueRow(db, "plugin_version", [plugin_id: pluginId,
                                             version_string: versionString])
}

def addInstanceRecord(Sql db, Integer instanceId, Integer containerId, Integer versionId,
                      String whenSeen) {
//    def whenSeen = whenSeenDate.format("yyyy-MM-dd HH:mm:ss Z")

//    def existingRow = getIDFromQuery(db, "select id from instance_record where instance_id = ${instanceId} and when_seen = '${whenSeen}'")
//    if (existingRow == null) {
    return addUniqueRow(db, "instance_record", [instance_id: instanceId,
                                                servlet_container_id: containerId,
                                                jenkins_version_id: versionId,
                                                when_seen: whenSeen])
/*    } else {
        return existingRow
    }*/
}

def addJobRecord(BatchingStatementWrapper stmt, Integer recordId, Integer jobTypeId,
                 Integer jobCount) {
    addRow(stmt, "job_record", [
        instance_record_id: recordId,
        job_type_id: jobTypeId,
        job_count: jobCount
    ])

}

def addNodeRecord(BatchingStatementWrapper stmt, Integer recordId,
                  Integer jvmId, Integer osId, Boolean master, Integer executors) {
    def fieldsToAdd = [
        instance_record_id: recordId,
        os_id: osId,
        master: master, executors: executors
    ]

    if (jvmId != null) {
        fieldsToAdd."jvm_id" = jvmId
    }
    addRow(stmt, "node_record", fieldsToAdd)
}

def addPluginRecord(BatchingStatementWrapper stmt, Integer recordId, Integer pluginVersionId) {
    addRow(stmt, "plugin_record", [
        instance_record_id: recordId,
        plugin_version_id: pluginVersionId
    ])
}

def failOrNot(Sql db, String query) {
    try {
        //true
        db.execute(query)
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
when_seen TIMESTAMP with time zone,
CONSTRAINT unique_instance_record UNIQUE(instance_id, when_seen)
);
""")

    failOrNot(db, "CREATE INDEX instance_record_idx ON instance_record (instance_id, when_seen);")


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

def process(String timestamp, File logDir) {
    Sql.LOG.level = java.util.logging.Level.SEVERE
    Sql db = Sql.newInstance("jdbc:postgresql://localhost:5432/usageDb", "stats", "admin", "org.postgresql.Driver")

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
    def noJobs = [:]

    def newWhenSeen = Date.parse("yyyyMM", timestamp).format("yyyy-MM-dd HH:mm:ss Z", TimeZone.getTimeZone("GMT"))
    logDir.eachFileMatch(~/$logRE/) { origGzFile ->
        if (db.rows("select * from seen_logs where filename = ${origGzFile.name}").isEmpty()) {
            db.execute("insert into seen_logs values (${origGzFile.name})")
            println "Handing original log ${origGzFile.canonicalPath}"
            new GZIPInputStream(new FileInputStream(origGzFile)).eachLine("UTF-8") { l ->
                linesSeen++;
                def j = slurper.parseText(l)
                def installId = j.install
                def ver = j.version
                if (!noJobs.containsKey(installId)) {
                    noJobs[installId] = 1
                } else {
                    noJobs[installId]++
                }
                def jobCnt = j.jobs.values().inject(0) { acc, val -> acc + val }
                j.whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", j.timestamp)
                if (jobCnt > 0) {
                    if (!instColl.containsKey(installId)) {
                        instColl[installId] = [1, j]
                    } else if (j.whenSeen > instColl[installId][1].whenSeen) {
                        def runningCnt = instColl[installId][0] + 1
                        instColl[installId] = [runningCnt, j]
                    }
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
    def alreadySeenInstalls = [:]
    def alreadySeenVersions = [:]
    def alreadySeenPluginIds = [:]
    def alreadySeenContainers = [:]
    def alreadySeenJvms = [:]
    def alreadySeenOses = [:]
    def alreadySeenJobTypes = [:]
    def alreadySeenPluginVersions = [:]

    db.connection.autoCommit = false
    def moreThanOne = instColl.findAll { k, v -> v[0] >= 2 }

    println "Adding ${moreThanOne.size()} instances (${recCnt} records)"
    noJobs.sort { a, b -> a.value <=> b.value }.each { k, v ->
        println " -- ${k}: ${v} jobs"
    }
    instColl = [:]
    try {
        db.withBatch { stmt ->
            moreThanOne.values().collect { it[1] }.each { j ->
                def installIdStr = j.install
                def ver = j.version

                def installId
                def jenkinsVersionId
                def containerId

                if (!alreadySeenInstalls.containsKey(installIdStr)) {
                    installId = getIDFromQuery(db, "instance", [identifier: installIdStr])
                    if (installId == null) {
                        installId = instanceRowId(db, installIdStr)
                    }
                    alreadySeenInstalls[installIdStr] = installId
                } else {
                    installId = alreadySeenInstalls[installIdStr]
                }

                if (!alreadySeenVersions.containsKey(ver)) {
                    jenkinsVersionId = getIDFromQuery(db, "jenkins_version", [version_string: ver])
                    if (jenkinsVersionId == null) {
                        jenkinsVersionId  = jenkinsVersionRowId(db, ver)
                    }
                    alreadySeenVersions[ver] = jenkinsVersionId
                } else {
                    jenkinsVersionId = alreadySeenVersions[ver]
                }

                if (!alreadySeenContainers.containsKey(j.servletContainer)) {
                    containerId = getIDFromQuery(db, "servlet_container", [container_name: j.servletContainer])
                    if (containerId == null) {
                        containerId  = containerRowId(db, j.servletContainer)
                    }
                    alreadySeenContainers[j.servletContainer] = containerId
                } else {
                    containerId = alreadySeenContainers[j.servletContainer]
                }

                def recordId = addInstanceRecord(db, installId, containerId, jenkinsVersionId, newWhenSeen)

                j.nodes?.each { n ->
                    def jvmName = n.containsKey("jvm-name") && !(n."jvm-name" instanceof Boolean) ? n."jvm-name" : null
                    def jvmVersion = n.containsKey("jvm-version") && !(n."jvm-version" instanceof Boolean) ? n."jvm-version" : null
                    def jvmVendor = n.containsKey("jvm-vendor") && !(n."jvm-vendor" instanceof Boolean) ? n."jvm-vendor" : null

                    def jvmId
                    def osId

                    if (jvmName != null && jvmVersion != null && jvmVendor != null) {
                        if (!alreadySeenJvms.containsKey([jvmName, jvmVersion, jvmVendor])) {
                            jvmId = getIDFromQuery(db, "jvm", [jvm_name: jvmName, jvm_version: jvmVersion, jvm_vendor: jvmVendor])
                            if (jvmId == null) {
                                jvmId  = jvmRowId(db, jvmName, jvmVersion, jvmVendor)
                            }
                            alreadySeenJvms[[jvmName, jvmVersion, jvmVendor]] = jvmId
                        } else {
                            jvmId = alreadySeenJvms[[jvmName, jvmVersion, jvmVendor]]
                        }
                    }
                    def isMaster = n.master ?: false
                    if (!alreadySeenOses.containsKey(n.os)) {
                        osId = getIDFromQuery(db, "os", [os_name: n.os])
                        if (osId == null) {
                            osId  = osRowId(db, n.os)
                        }
                        alreadySeenOses[n.os] = osId
                    } else {
                        osId = alreadySeenOses[n.os]
                    }

                    def executors = n.executors

                    addNodeRecord(stmt, recordId, jvmId, osId, isMaster, executors)

                }

                j.plugins?.each { p ->
                    def pluginId
                    def pluginVersionId
                    if (!alreadySeenPluginIds.containsKey(p.name)) {
                        pluginId = getIDFromQuery(db, "plugin", [plugin_name: p.name])
                        if (pluginId == null) {
                            pluginId  = pluginRowId(db, p.name)
                        }
                        alreadySeenPluginIds[p.name] = pluginId
                    } else {
                        pluginId = alreadySeenPluginIds[p.name]
                    }

                    if (!alreadySeenPluginVersions.containsKey([pluginId, p.version])) {
                        pluginVersionId = getIDFromQuery(db, "plugin_version", [plugin_id: pluginId, version_string: p.version])
                        if (pluginVersionId == null) {
                            pluginVersionId = pluginVersionRowId(db, p.version, pluginId)
                        }
                        alreadySeenPluginVersions[[pluginId, p.version]] = pluginVersionId
                    } else {
                        pluginVersionId = alreadySeenPluginVersions[[pluginId, p.version]]
                    }

                    if (!alreadySeenPlugins.containsKey([installIdStr, p.name, p.version])) {
                        addPluginRecord(stmt, recordId, pluginVersionId)
                        alreadySeenPlugins[[installIdStr, p.name, p.version]] = true
                    }
                }

                j.jobs?.each { type, cnt ->
                    if (!alreadySeenJobTypes.containsKey(type)) {
                        jobTypeId = getIDFromQuery(db, "job_type", [class_name: type])
                        if (jobTypeId == null) {
                            jobTypeId  = jobTypeRowId(db, type)
                        }
                        alreadySeenJobTypes[type] = jobTypeId
                    } else {
                        jobTypeId = alreadySeenJobTypes[type]
                    }

                    if (!alreadySeenJobs.containsKey([installIdStr, jobTypeId])) {
                        addJobRecord(stmt, recordId, jobTypeId, cnt)
                        alreadySeenJobs[[installIdStr, jobTypeId]] = true
                    }
                }

            }
        }
    } catch (BatchUpdateException e) {
        throw e.getNextException()
    }
    db.connection.commit()
}
