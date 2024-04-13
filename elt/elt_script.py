import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5): 
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready","-h",host], check=True, capture_output=True, text=True
                )
            
            '''
                ["pg_isready", "-h", host]: This is the command you want to execute in the subprocess. 
                It's a list where the first element is the command (pg_isready) and subsequent elements are command-line arguments 
                (-h followed by the host variable).

                check=True: This parameter makes subprocess.run() raise a CalledProcessError if the executed command 
                returns a non-zero exit status, indicating an error. If check=False, it won't raise an error and return a CompletedProcess object.

                capture_output=True: This parameter tells subprocess.run() to capture the output (both stdout and stderr) of the command.

                text=True: This parameter tells subprocess.run() to treat the captured output as text (str) rather than bytes.
            '''

            if "accepting connections" in result.stdout:
                print("Successfully connected to postgres")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to postgres: {e}")
            retries +=1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})"
            )
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting")
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT Script. . .")

source_config = {
    'db_name': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'db_name': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

'''
By attaching both services to the same network, Docker ensures that they can communicate with each other using their service names as hostnames. 
In this case, the elt_script service can simply use source_postgres as the hostname to connect to the PostgreSQL server.
'''

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['db_name'],
    '-f', 'data_dump.sql',
    '-w'
]

'''
'pg_dump': This is the command to execute, which is pg_dump, the PostgreSQL utility for performing database dumps.

'-h', source_config['host']: This specifies the hostname or IP address of the PostgreSQL server to connect to. 
The value is fetched from the source_config dictionary, which likely contains configuration parameters for connecting to the source database.

'-U', source_config['user']: This specifies the username to use when connecting to the PostgreSQL database. 
Similar to the hostname, the value is fetched from the source_config dictionary.

'-d', source_config['db_name']: This specifies the name of the database to dump. 
Again, the value is fetched from the source_config dictionary.

'-f', 'data_dump.sql': This specifies the filename and location to use for the output SQL file that will contain the dumped database schema 
and data. In this case, the file will be created in the current working directory where the script is executed.

'-w': This flag tells pg_dump not to prompt for a password. 
It's used when authentication is handled in a different way, such as through a .pgpass file or environment variables.
'''

subprocess_env = dict(PGPASSWORD=source_config['password'])

subprocess.run(dump_command, env=subprocess_env, check=True)

load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['db_name'],
    '-a','-f','data_dump.sql'
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

'''
'psql': This is the command to execute, which is psql, the PostgreSQL utility for running SQL commands.

'-h', destination_config['host']: This specifies the hostname or IP address of the PostgreSQL server to connect to. 
The value is fetched from the destination_config dictionary, which likely contains configuration parameters for 
connecting to the destination database.

'-U', destination_config['user']: This specifies the username to use when connecting to the PostgreSQL database. 
Similar to the hostname, the value is fetched from the destination_config dictionary.

'-d', destination_config['db_name']: This specifies the name of the database to connect to. 
Again, the value is fetched from the destination_config dictionary.

'-a': This is a flag that tells psql to echo all input from scripts.

'-f', 'data_dump.sql': This specifies the filename of the SQL file to execute. 
In this case, it's set to 'data_dump.sql', which is assumed to be in the current working directory where the psql command is executed.
'''

subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT Script. . .")