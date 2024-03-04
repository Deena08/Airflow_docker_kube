

import snowflake.connector

SNOWFLAKE_ACCOUNT = 'GO57213'
SNOWFLAKE_USER = 'DEENA'
SNOWFLAKE_PASSWORD = 'd@9551543067D'
SNOWFLAKE_DATABASE = 'DEENA_DB'
SNOWFLAKE_WAREHOUSE = 'DEENA_WH'

def login_and_print_message():
    conn = snowflake.connector.connect(
        user='DEENA',
        password='d@9551543067D',
        account='npezbbo-go57213',
        warehouse='DEENA_WH',
        database='DEENA_DB'
    )

    cursor = conn.cursor()
    cursor.execute("SELECT current_version()")
    version = cursor.fetchone()[0]
    print(f"Connected to Snowflake. Snowflake Version: {version}")
    conn.close()

if __name__ == "__main__":
    login_and_print_message()




