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

if (argResult.incremental) {
    byMonth=[:] as TreeMap
    re = /.*log\.([0-9]{6})[0-9]+\.gz/ 
    logDir.eachFileMatch(~re) { f ->
        m = (f=~re)
        if (m)  byMonth[m[0][1]] = true;
    }
    def data = byMonth.keySet() as List
    println "Found logs: ${data}"

    // do not process the current month as the data may not be complete yet
    data.pop()
    data.each { t ->
        if (new File(outputDir,"${t}.json.gz").exists()) {
            println "Skipping ${t}.json.gz as it already exists"
        } else {
            process(t,logDir,outputDir, mongoPort);
        }
    }
} else {
    // just process one month specified in the command line
    if (argResult.timestamp==null)
        throw new Error("Neither --incremental nor --timestamp was specified");
    process(argResult.timestamp, logDir, outputDir, mongoPort);
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

def addRow(Sql db, String table, Map<String,Object> fields) {
    return db.executeInsert("insert into ${table} (${fields.keySet().join(',')}) values (${fields.values().collect { "'" + it + "'" }.join(',')})".toString())[0][0]
}

def getRowId(Sql db, String table, String field, String value) {
    def id = getIDFromQuery(db, "select id from ${table} where ${field} = '${value}'")
    if (id == null) {
        id = addRow(db, table, field, value)
        println "adding ${table} ${value} to id ${id}"
    }
    return id
}

def getRowId(Sql db, String table, Map<String,Object> fields) {
    def id = getIDFromQuery(db, "select id from ${table} where ${fields.collect { "${it.key} = '${it.value}'" }.join(" AND ") }")
    if (id == null) {
        id = addRow(db, table, fields)
        println "adding ${table} ${fields} to id ${id}"
    }
    return id
}

def instanceRowId(Sql db, String instanceId) {
    return getRowId(db, "instance", "identifier", instanceId)
}

def jenkinsVersionRowId(Sql db, String versionString) {
    return getRowId(db, "jenkins_version", "version_string", versionString)
}

def containerRowId(Sql db, String containerString) {
    return getRowId(db, "servlet_container", "container_name", containerString)
}

def jobTypeRowId(Sql db, String className) {
    return getRowId(db, "job_type", "class_name", className)
}

def jvmRowId(Sql db, String jvmName, String jvmVersion, String jvmVendor) {
    return getRowId(db, "jvm", [jvm_name: jvmName, jvm_version: jvmVersion, jvm_vendor: jvmVendor])
}

def osRowId(Sql db, String osName) {
    return getRowId(db, "os", "os_name", osName)
}

def pluginRowId(Sql db, String pluginName) {
    return getRowId(db, "plugin", "plugin_name", pluginName)
}

def pluginVersionRowId(Sql db, String versionString, int pluginId) {
    return getRowId(db, "plugin_version", [plugin_id: "${pluginId}", version_string: versionString])
}

def addInstanceRecord(Sql db, int instanceId, int containerId, int jenkinsVersionId, String dateString) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss zzz")
    return getRowId(db, "instance_record", [instance_id: instanceId, container_id: containerId, jenkins_version_id: jenkinsVersionId,
                                            when_seen: whenSeen])
}

def addJobRecord(Sql db, int instanceRecordId, int jobTypeId, int jobCount) {
    addRow(db, "job_record", [instance_record_id: instanceRecordId, job_type_id: jobTypeId, job_count: jobCount])
    println "adding job record for instance record ${instanceRecordId} and job type record ${jobTypeId}"
}

def addNodeRecord(Sql db, int instanceRecordId, int jvmId, int osId, Boolean master, int executors) {
    addRow(db, "node_record", [instance_record_id: instanceRecordId, jvm_id: jvmId, os_id: osId, master: master, executors: executors])
    println "adding node record for instance record ${instanceRecordId} and some node"
}

def addPluginRecord(Sql db, int instanceRecordId, int pluginVersionId) {
    addRow(db, "plugin_record", [instance_record_id: instanceRecordId, plugin_version_id: pluginVersionId])
    println "adding plugin record for instance record ${instanceRecordId} and plugin version ${pluginVersionId}"
}

def createTablesIfNeeded(Sql db) {
    db.execute("""CREATE TABLE IF NOT EXISTS instance (
id SERIAL PRIMARY KEY,
identifier varchar(64),
CONSTRAINT unique_id UNIQUE(identifier)
);
""")

    db.execute("CREATE INDEX instance_identifier_idx ON instance (identifier);")

    db.execute("""CREATE TABLE IF NOT EXISTS servlet_container (
id SERIAL PRIMARY KEY,
container_name varchar,
CONSTRAINT unique_container UNIQUE(container_name)
);
""")

    db.execute("CREATE INDEX container_name_idx ON servlet_container (container_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS jenkins_version (
id SERIAL PRIMARY KEY,
version_string varchar,
CONSTRAINT unique_version UNIQUE(version_string)
);
""")

    db.execute("CREATE INDEX jenkins_version_idx ON jenkins_version (version_string);")

    db.execute("""CREATE TABLE IF NOT EXISTS job_type (
id SERIAL PRIMARY KEY,
class_name varchar,
CONSTRAINT unique_type UNIQUE(class_name)
);
""")

    db.execute("CREATE INDEX job_type_idx ON job_type (class_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS jvm (
id SERIAL PRIMARY KEY,
jvm_name varchar,
jvm_version varchar,
jvm_vendor varchar,
CONSTRAINT unique_jvm UNIQUE(jvm_name, jvm_version, jvm_vendor)
);
""")

    db.execute("CREATE INDEX jvm_idx ON jvm (jvm_name, jvm_version, jvm_vendor);")

    db.execute("""CREATE TABLE IF NOT EXISTS os (
id SERIAL PRIMARY KEY,
os_name varchar,
CONSTRAINT unique_os UNIQUE(os_name)
);
""")

    db.execute("CREATE INDEX os_idx ON os (os_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin (
id SERIAL PRIMARY KEY,
plugin_name varchar,
CONSTRAINT unique_plugin UNIQUE(plugin_name)
);
""")

    db.execute("CREATE INDEX plugin_idx ON plugin (plugin_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin_version (
id SERIAL PRIMARY KEY,
plugin_id SERIAL references plugin(id),
version_string varchar,
CONSTRAINT unique_plugin_version UNIQUE(plugin_id, version_string)
);
""")

    db.execute("CREATE INDEX plugin_version_idx ON plugin_version (plugin_id, version_string);")

    db.execute("""CREATE TABLE IF NOT EXISTS instance_record (
id SERIAL PRIMARY KEY,
instance_id SERIAL REFERENCES instance(id),
servlet_container_id SERIAL REFERENCES servlet_container(id),
jenkins_version_id SERIAL REFERENCES jenkins_version(id),
when_seen TIMESTAMP with time zone,
CONSTRAINT unique_instance_record UNIQUE(instance_id, when_seen)
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS job_record (
id SERIAL PRIMARY KEY,
instance_record_id SERIAL references instance_record(id),
job_type_id SERIAL references job_type(id),
job_count integer,
CONSTRAINT unique_job_record UNIQUE(instance_record_id, job_type_id)
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS node_record (
id SERIAL PRIMARY KEY,
instance_record_id SERIAL references instance_record(id),
jvm_id SERIAL references jvm(id),
os_id SERIAL references os(id),
master boolean,
executors integer
);
""")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin_record (
id SERIAL PRIMARY KEY,
instance_record_id SERIAL references instance_record(id),
plugin_version_id SERIAL references plugin_version(id),
CONSTRAINT unique_plugin_record UNIQUE(instance_record_id, plugin_version_id)
);
""")

}

def process(String timestamp/*such as '201112'*/, File logDir, File outputDir, int mongoPort) {
    Sql db = Sql.newInstance("jdbc:postgresql://localhost:5432/usageDb", "stats", "admin", "org.postgresql.Driver")
    createTablesIfNeeded(db)


    def procJson = [:]

    def ant = new AntBuilder()

    def slurper = new JsonSlurper()

    def tmpDir = new File("./tmp")

    if (!tmpDir.isDirectory()) { 
        tmpDir.mkdirs()
    }

    def logRE = ".*log\\.${timestamp}.*gz"

    def linesSeen = 0
    def instCnt = [:]

    logDir.eachFileMatch(~/$logRE/) { origGzFile ->
        println "Handing original log ${origGzFile.canonicalPath}"
        new GZIPInputStream(new FileInputStream(origGzFile)).eachLine("UTF-8") { l ->
            linesSeen++;
            def j = slurper.parseText(l)
            def installId = j.install
            def ver = j.version

            def jobCnt = j.jobs.values().inject(0) { acc, val -> acc+ val }

            if (jobCnt > 0) {
                def instRowId = instanceRowId(db, installId)
                def verId = jenkinsVersionRowId(db, ver)
                def containerId = containerRowId(db, j.servletContainer)

                def recordId = addInstanceRecord(db, instRowId, containerId, verId, j.timestamp)

                j.nodes?.each { n ->
                    Integer jvmId
                    if (n."jvm-name" != null && n."jvm-version" != null && n."jvm-vendor" != null) {
                        jvmId = jvmRowId(db, n."jvm-name", n."jvm-version", n."jvm-vendor")
                    }
                    def isMaster = n.master ?: false
                    def osId = osRowId(db, n.os)
                    def executors = n.executors
                    addNodeRecord(db, recordId, jvmId, osId, isMaster, executors)
                }

                j.plugins?.each { p ->
                    def pluginId = pluginRowId(db, p.name)
                    def pluginVersionId = pluginVersionRowId(db, p.version, pluginId)
                    addPluginRecord(db, recordId, pluginVersionId)
                }

                j.jobs?.each { type, cnt ->
                    def jobTypeId = jobTypeRowId(db, type)
                    addJobRecord(db, recordId, jobTypeId, cnt)
                }
            }
            
        }
    }

}
