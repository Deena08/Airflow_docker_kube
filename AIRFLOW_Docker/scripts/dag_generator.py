from jinja2 import Environment, FileSystemLoader
import yaml
import os

class dag_generator:
   
 def dag_gen():
  file_dir = os.path.dirname(os.path.abspath(f"{__file__}/../"))
  env = Environment(loader=FileSystemLoader(file_dir))
  template = env.get_template('templates/dag_template.jinja2')

  for filename in os.listdir(f"{file_dir}/inputs"):
    print(filename)
    if filename.endswith('.yaml'):
        with open(f"{file_dir}/inputs/{filename}","r") as input_file:
            inputs=yaml.safe_load(input_file)
            with open(f"/home/dhinakaran/Documents/AIRFLOW_Docker/dags/{inputs['dag_id']}.py","w") as f:
                f.write(template.render(inputs))

 if __name__ == "__main__":
  
  dag_gen() 