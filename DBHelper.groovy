import org.sqlite.*
import java.sql.*

class DBHelper {

    def static setupDB(workingDir) {

        def dbFile = new File(workingDir, "stats.db")


        // in memory
        // db = groovy.sql.Sql.newInstance("jdbc:sqlite::memory:","org.sqlite.JDBC")

        // persistent
        def db = groovy.sql.Sql.newInstance("jdbc:mysql://localhost:3306/stats",
            "abayer",
            '$Hrugg3d',
            "com.mysql.jdbc.Driver")

        // define the tables
        db.execute("create table if not exists startedfiles(name VARCHAR(64), started DATETIME, finished DATETIME)")
        db.execute("create table if not exists instance_month(id int auto_increment primary key, instanceid VARCHAR(64), month DATE, INDEX instance_month_instid (instanceid), INDEX instance_month_month (month))")
        db.execute("create table if not exists jenkins(instid int, version VARCHAR(128), jvmvendor VARCHAR(64), jvmname VARCHAR(64), jvmversion VARCHAR(128), " +
            "numberofexecutors int, INDEX jenkins_version (version), INDEX jenkins_instid (instid), FOREIGN KEY (instid) references instance_month(id));")
        db.execute("create table if not exists plugin(instid int, name VARCHAR(255), version VARCHAR(128), " +
            "INDEX plugin_name (name), INDEX plugin_nameinst (name, instid), FOREIGN KEY (instid) references instance_month(id));")
        db.execute("create table if not exists job(instid int, type VARCHAR(512), jobnumber int, INDEX job_instid (instid));")
        db.execute("create table if not exists node(instid int, osname VARCHAR(64), nodenumber int, INDEX node_instid (instid))")
/*            db.execute("CREATE INDEX plugin_name on plugin (name)")
            db.execute("CREATE INDEX jenkins_version on jenkins (version)")
            db.execute("CREATE INDEX jenkins_month on jenkins (month)")
            db.execute("CREATE INDEX plugin_month on plugin (month)")
            db.execute("CREATE INDEX plugin_namemonth on plugin (name,month)")*/

        return db;
    }


    /**
     * is the file with the given name already imported?
     */
    static boolean doImport(db, fileName){
        if(db){
            def filePrefix = fileName.substring(0, fileName.indexOf("."))+"%"
            def rows = db.rows("select name from startedfiles where name like $filePrefix and finished is not null;")
            return rows.size() == 0
        }
        true
    }

}



