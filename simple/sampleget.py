from airflow.models import Variable
import requests

def get_airflow_variable(key):
    try:
      
        url = f"http://localhost:8080/api/v1/variables/{key}"
        response = requests.get(url, auth=('airflow', 'airflow'))
        
        if response.status_code == 200:
            variable_value = response.json().get('value')
            return variable_value
        else:
            print("Failed to fetch variable. Status code:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", str(e))
        return None


user_variable = get_airflow_variable("sample")
print("User variable value:", user_variable)
