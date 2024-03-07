
from flask import Flask
import snowflake.connector

app = Flask(__name__)

@app.route('/')
def execute_job():
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
    conn.close()
    
    return 'Snowflake job executed'

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081)




