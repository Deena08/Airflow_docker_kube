
FROM python:3.9

RUN pip install snowflake-connector-python

RUN pip install pandas

RUN pip install numpy

RUN pip install --upgrade numpy pandas

RUN pip install numexpr==2.7.3

RUN pip install --upgrade numexpr

RUN pip install flask

COPY snowflake_login.py /snowflake_login.py

CMD ["python3", "/snowflake_login.py"]
