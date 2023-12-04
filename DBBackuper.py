import subprocess
import mysql.connector
from mysql.connector import Error

def backup_mysql(source_params, backup_file):
    try:
        connection = mysql.connector.connect(**source_params)
        cursor = connection.cursor()

        # Use MySQL query to dump database structure and data to a file
        with open(backup_file, 'w') as file:
            cursor.execute('SHOW TABLES')
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                cursor.execute(f'SHOW CREATE TABLE {table_name}')
                create_table_sql = cursor.fetchone()[1]
                file.write(f'{create_table_sql};\n')

                cursor.execute(f'SELECT * FROM {table_name}')
                for row in cursor.fetchall():
                    values = ', '.join(f"'{value}'" if value is not None else 'NULL' for value in row)
                    file.write(f'INSERT INTO {table_name} VALUES ({values});\n')

        print(f"Backup saved to {backup_file}")

    except Error as e:
        print(f"Error: {e}")
        raise

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return cursor.fetchone() is not None

def load_mysql(destination_params, backup_file):
    try:
        conn_destination = mysql.connector.connect(**destination_params)
        cursor_destination = conn_destination.cursor()

        # Disable foreign key checks
        cursor_destination.execute("SET foreign_key_checks = 0")

        with open(backup_file, 'r') as file:
            sql_script = file.read()

        # Split the SQL script into separate statements
        sql_statements = sql_script.split(';')

        for statement in sql_statements:
            if statement.strip():  # Skip empty statements
                try:
                    cursor_destination.execute(statement)
                except mysql.connector.errors.ProgrammingError as pe:
                    if pe.errno == 1050:  # Table already exists
                        table_name = statement.split()[2].strip('`')  # Extract the table name
                        cursor_destination.execute(f"DROP TABLE IF EXISTS {table_name}")
                        cursor_destination.execute(statement)
                    else:
                        print(f"Error: {pe}")
                        raise

        # Re-enable foreign key checks
        cursor_destination.execute("SET foreign_key_checks = 1")

        conn_destination.commit()
        print("Backup loaded to the destination database")

    except Error as e:
        print(f"Error: {e}")
        raise

    finally:
        if conn_destination.is_connected():
            cursor_destination.close()
            conn_destination.close()


def test_mysql_connection(source_params):
    connection = None

    try:
        connection = mysql.connector.connect(**source_params)
        if connection.is_connected():
            print(f"Connected to MySQL database on {source_params['host']} as {source_params['user']}")
    except OSError as e:
        print(f"Error: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Connection closed.")

def main():
    source_db_params = {
        'host': '192.168.1.68',
        'user': 'adam',
        'password': 'PrukalaAPSC22#',
        'database': 'BASDB2',
    }

    destination_db_params = {
        'host': '192.168.1.68',
        'user': 'adam',
        'password': 'PrukalaAPSC22#',
        'database': 'BASDB',
    }
    print("Check source database parameters (REQUIRED FOR BACKUP TO FILE)")
    print(source_db_params)
    print("Check destination database parameters (REQUIRED FOR BACKUP FROM FILE)")
    print(destination_db_params)
    action = input("Parameters ok? Yes/No")
    if action == 'yes':
        action = input("Choose action (backup/load/test): ").lower()

        if action == 'backup':
            backup_file = input("Enter backup file name: (no extensions)") + ".sql"
            backup_mysql(source_db_params, backup_file)
            print(f"Backup saved to {backup_file}")

        elif action == 'load':
            backup_file = input("Enter backup file name: (no extensions)") + ".sql"
            load_mysql(destination_db_params, backup_file)
            print("Backup loaded to the destination database")
        elif action == 'test':
            action = input("Choose credentials (source/destination): ").lower()
            if action == 'source':
                test_mysql_connection(source_db_params)
            elif action == 'destination':
                test_mysql_connection(destination_db_params)

        else:
            print("Invalid action. Please choose 'backup' or 'load'.")

if __name__ == "__main__":
    main()
