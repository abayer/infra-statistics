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

def instanceRowId(DataSet instances, String instanceId) {
    if (instances.findAll { it.identifier == instanceId }.firstRow() == null) {
        instances.add(identifier: instanceId)
        println "adding instance ${instanceId}"
    }
    return instances.findAll { it.identifier == instanceId }.firstRow().id
}

def jenkinsVersionRowId(DataSet jenkinsVersions, String versionString) {
    if (jenkinsVersions.findAll { it.version_string == versionString }.firstRow() == null) {
        jenkinsVersions.add(version_string: versionString)
        println "adding version ${versionString}"
    }
    return jenkinsVersions.findAll { it.version_string == versionString }.firstRow().id
}

def containerRowId(DataSet containers, String containerString) {
    if (containers.findAll { it.container_name == containerString }.firstRow() == null) {
        containers.add(container_name: containerString)
        println "adding container ${containerString}"
    }
    return containers.findAll { it.container_name == containerString }.firstRow().id
}

def jobTypeRowId(DataSet jobTypes, String className) {
    if (jobTypes.findAll { it.class_name == className }.firstRow() == null) {
        jobTypes.add(class_name: className)
        println "adding job type ${className}"
    }
    return jobTypes.findAll { it.class_name == className }.firstRow().id
}

def jvmRowId(DataSet jvms, String jvmName, String jvmVersion, String jvmVendor) {
    if (jvms.findAll { it.jvm_name == jvmName && it.jvm_version == jvmVersion && it.jvm_vendor == jvmVendor }.firstRow() == null) {
        jvms.add(jvm_name: jvmName, jvm_version: jvmVersion, jvm_vendor: jvmVendor)
        println "adding jvm ${jvmName}:${jvmVersion}:${jvmVendor}"
    }
    return jvms.findAll { it.jvm_name == jvmName && it.jvm_version == jvmVersion && it.jvm_vendor == jvmVendor }.firstRow().id
}

def osRowId(DataSet oses, String osName) {
    if (oses.findAll { it.os_name == osName }.firstRow() == null) {
        oses.add(os_name: osName)
        println "adding os ${osName}"
    }
    return oses.findAll { it.os_name == osName }.firstRow().id
}

def pluginRowId(DataSet plugins, String pluginName) {
    if (plugins.findAll { it.plugin_name == pluginName }.firstRow() == null) {
        plugins.add(plugin_name: pluginName)
        println "adding plugin ${pluginName}"
    }
    return plugins.findAll { it.plugin_name == pluginName }.firstRow().id
}

def pluginVersionRowId(DataSet pluginVersions, String versionString, int pluginId) {
    if (pluginVersions.findAll { it.version_string == versionString && it.plugin_id == pluginId }.firstRow() == null) {
        pluginVersions.add(version_string: versionString, plugin_id: pluginId)
        println "adding plugin version ${versionString} for ${pluginId}"
    }
    return pluginVersions.findAll { it.version_string == versionString && it.plugin_id == pluginId }.firstRow().id
}

def addInstanceRecord(DataSet instanceRecords, int instanceId, int containerId, int jenkinsVersionId, String dateString) {
    def whenSeen = Date.parse("dd/MMM/yyyy:H:m:s Z", dateString).format("yyyy-MM-dd HH:mm:ss zzz")
    instanceRecords.add(instance_id: instanceId, container_id: containerId, jenkins_version_id: jenkinsVersionId,
        when_seen: whenSeen)
    println "adding instance record for ${instanceId}"
    return instanceRecords.findAll { it.instance_id == instanceId && it.when_seen == whenSeen }.firstRow().id
}

def addJobRecord(DataSet jobRecords, int instanceRecordId, int jobTypeId, int jobCount) {
    jobRecords.add(instance_record_id: instanceRecordId, job_type_id: jobTypeId, job_count: jobCount)
    println "adding job record for instance record ${instanceRecordId} and job type record ${jobTypeId}"
}

def addNodeRecord(DataSet nodeRecords, int instanceRecordId, int jvmId, int osId, Boolean master, int executors) {
    nodeRecords.add(instance_record_id: instanceRecordId, jvm_id: jvmId, os_id: osId, master: master, executors: executors)
    println "adding node record for instance record ${instanceRecordId} and some node"
}

def addPluginRecord(DataSet pluginRecords, int instanceRecordId, int pluginVersionId) {
    pluginRecords.add(instance_record_id: instanceRecordId, plugin_version_id: pluginVersionId)
    println "adding plugin record for instance record ${instanceRecordId} and plugin version ${pluginVersionId}"
}

def createTablesIfNeeded(Sql db) {
    db.execute("""CREATE TABLE IF NOT EXISTS instance (
id SERIAL PRIMARY KEY,
identifier varchar(64),
CONSTRAINT unique_id UNIQUE(identifier)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS instance_identifier_idx ON instance (identifier);")

    db.execute("""CREATE TABLE IF NOT EXISTS servlet_container (
id SERIAL PRIMARY KEY,
container_name varchar,
CONSTRAINT unique_container UNIQUE(container_name)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS container_name_idx ON servlet_container (container_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS jenkins_version (
id SERIAL PRIMARY KEY,
version_string varchar,
CONSTRAINT unique_version UNIQUE(version_string)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS jenkins_version_idx ON jenkins_version (version_string);")

    db.execute("""CREATE TABLE IF NOT EXISTS job_type (
id SERIAL PRIMARY KEY,
class_name varchar,
CONSTRAINT unique_type UNIQUE(class_name)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS job_type_idx ON job_type (class_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS jvm (
id SERIAL PRIMARY KEY,
jvm_name varchar,
jvm_version varchar,
jvm_vendor varchar,
CONSTRAINT unique_jvm UNIQUE(jvm_name, jvm_version, jvm_vendor)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS jvm_idx ON jvm (jvm_name, jvm_version, jvm_vendor);")

    db.execute("""CREATE TABLE IF NOT EXISTS os (
id SERIAL PRIMARY KEY,
os_name varchar,
CONSTRAINT unique_os UNIQUE(os_name)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS os_idx ON os (os_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin (
id SERIAL PRIMARY KEY,
plugin_name varchar,
CONSTRAINT unique_plugin UNIQUE(plugin_name)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS plugin_idx ON plugin (plugin_name);")

    db.execute("""CREATE TABLE IF NOT EXISTS plugin_version (
id SERIAL PRIMARY KEY,
plugin_id SERIAL references plugin(id),
version_string varchar,
CONSTRAINT unique_plugin_version UNIQUE(plugin_id, version_string)
);
""")

    db.execute("CREATE INDEX IF NOT EXISTS plugin_version_idx ON plugin_version (plugin_id, version_string);")

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

    DataSet instances = db.dataSet("instance")
    DataSet containers = db.dataSet("servlet_container")
    DataSet jenkinsVersions = db.dataSet("jenkins_version")
    DataSet jobTypes = db.dataSet("job_type")
    DataSet jvms = db.dataSet("jvm")
    DataSet oses = db.dataSet("os")
    DataSet plugins = db.dataSet("plugin")
    DataSet pluginVersions = db.dataSet("plugin_version")
    DataSet instanceRecords = db.dataSet("instance_record")
    DataSet jobRecords = db.dataSet("job_record")
    DataSet pluginRecords = db.dataSet("plugin_record")
    DataSet nodeRecords = db.dataSet("node_record")

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
                def instRowId = instanceRowId(instances, installId)
                def verId = jenkinsVersionRowId(jenkinsVersions, ver)
                def containerId = containerRowId(containers, j.servletContainer)

                def recordId = addInstanceRecord(instanceRecords, instRowId, containerId, verId, j.timestamp)

                j.nodes?.each { n ->
                    Integer jvmId
                    if (n."jvm-name" != null && n."jvm-version" != null && n."jvm-vendor" != null) {
                        jvmId = jvmRowId(jvms, n."jvm-name", n."jvm-version", n."jvm-vendor")
                    }
                    def isMaster = n.master ?: false
                    def osId = osRowId(oses, n.os)
                    def executors = n.executors
                    addNodeRecord(nodeRecords, recordId, jvmId, osId, isMaster, executors)
                }

                j.plugins?.each { p ->
                    def pluginId = pluginRowId(plugins, p.name)
                    def pluginVersionId = pluginVersionRowId(pluginVersions, p.version, pluginId)
                    addPluginRecord(pluginRecords, recordId, pluginVersionId)
                }

                j.jobs?.each { type, cnt ->
                    def jobTypeId = jobTypeRowId(jobTypes, type)
                    addJobRecord(jobRecords, recordId, jobTypeId, cnt)
                }
            }
            
        }
    }

}
