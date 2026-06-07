FROM apache/spark-py:v3.4.0

USER root

WORKDIR /opt/spark/work

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pandas

COPY app/wordcount.py /opt/spark/work/wordcount.py
COPY app/analysis.py /opt/spark/work/analysis.py
COPY app/sql_analysis.py /opt/spark/work/sql_analysis.py
COPY app/perf_compare.py /opt/spark/work/perf_compare.py
COPY douban_movies.csv /opt/spark/work/douban_movies.csv

RUN chmod -R 777 /opt/spark/work && ls -lh /opt/spark/work

USER root
