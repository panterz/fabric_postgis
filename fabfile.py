from fabric.api import task, env, prompt, local, sudo, run
import ConfigParser, ast


@task
def prepare(server='localhost'):
    """ prepare environment """
    env.server = server
    server_details = _get_param(server, 'server')

@task
def install_db(remote='False', opsystem='ubuntu'):
    """ install database, params remote=true"""
    #_prepare_env()
    remote = _str2bool(remote)
    #get details of db from config.ini
    db_details = _get_param(env.server, 'dbserver')
    
    user = prompt("Pass the name of the user of the db[leave blank for default, {0}]: ".format(db_details['user']))
    if user.lower() == '':
        user = db_details['user']

    port = prompt("Pass the port of the db [leave blank for default, {0}]: ".format(db_details['port']))
    if port.lower() == '':
        port = db_details['port']

    dbname = prompt("Pass the name of the db want installed[leave blank for default, {0}]: ".format(db_details['db']))
    if dbname.lower() == '':
        dbname = db_details['db']

    #check if db exists
    if "1" in _run_command(remote, "sudo -u postgres psql -p %s -l | grep %s | wc -l" % (db_details['port'], db_details['db']), True):
        print "The database already exists"
        an1 = prompt("Do you want me to delete the database?[y/n]")
        if an1.lower() == 'y':
            _run_command(remote, "sudo -u postgres dropdb -p %s %s" % (db_details['port'], db_details['db']))
    else:
        an1='y'

    #check if user exists
    if "1" in _run_command(remote, "sudo -u postgres psql -p %s postgres -tAc \"SELECT 1 FROM pg_roles WHERE rolname='%s'\"" % (port,user), capture=True):
        print "The user already exists"
        an2 = prompt("Do you want me to delete the user?[y/n]")
        if an2.lower() == 'y':
            _run_command(remote, "sudo -u postgres dropuser -p %s %s" % (db_details['port'], db_details['user']))
    else:
        an2='y'

    if an1.lower() == 'y':
        _run_command(remote, "sudo -u postgres createuser -P -p %s %s" % (db_details['port'], db_details['user']))

    #create database
    if an2.lower() == 'y':
        _run_command(remote, "sudo -u postgres createdb --port=%s --encoding=UTF8 --owner=%s %s"
                     % (db_details['port'], db_details['user'], db_details['db']))
        #local("sudo -u postgres createlang plpgsql %s" % dbname)  # note: this is obsolete with >=postgres-9.1
        if opsystem == "ubuntu":
            postgis_path = "/usr/share/postgresql/9.1/contrib/postgis-2.0/"
        else:
            postgis_path = "/usr/pgsql-9.3/share/contrib/postgis-2.1/"

        _run_command(remote, "sudo -u postgres psql -p %s -d %s -f %s/postgis.sql" % (port, dbname, postgis_path))
        _run_command(remote, "sudo -u postgres psql -p %s -d %s -f %s/spatial_ref_sys.sql" % (port, dbname, postgis_path))
        _run_command(remote, "sudo -u postgres psql -p %s -d %s -c 'GRANT SELECT ON spatial_ref_sys TO PUBLIC;'" % (port, dbname))
        _run_command(remote,"sudo -u postgres psql -p %s -d %s -c 'GRANT ALL ON geometry_columns TO %s;'" % (port, dbname, user))
        #_prepare_data(remote, env.app_name)

def _get_param(section, param):
    """get specific param"""
    config = ConfigParser.ConfigParser()
    config.read('etc/config.ini')
    return ast.literal_eval(config.get(section, param))

def _run_command(flag, cmd, capture=False):
    """run command either locally or remotely """
    if flag:
        return run(cmd, capture)
    else:
        return local(cmd, capture)

def _str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")