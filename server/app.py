import findspark

findspark.init()

import pandas as pd
from flask import Flask, jsonify
from pyspark.sql import SparkSession
import pyspark
import json
from flask import Response
from pyspark.sql.functions import sum, col, desc

spark = SparkSession.builder.master("local[1]").appName(
    "SparkByExamples.com").getOrCreate()

app = Flask(__name__)

dataset = spark.read.csv('data/WHO-COVID-19-global-data.csv',
                            inferSchema=True, header=True)
# Hello world API Route
@app.route("/", methods=["GET"])
def index():

    vndead = dataset.filter((dataset.Country_code == 'VN') & (
        dataset.New_cases > 0)).select('Date_reported', 'New_cases').distinct()
    pandasDF2 = vndead.toPandas()
    result = {}
    for index, row in pandasDF2.iterrows():
        #result[index] = row.to_json()
        result[index] = dict(row)
    return result


@app.route("/globaldeath", methods=["GET"])
def global_death():
    sumdeath = dataset.groupBy("Date_reported").agg(
        sum("New_deaths").alias("Deaths")).sort(desc("Date_reported"))
    pandasDF2 = sumdeath.toPandas()
    result = {}
    for index, row in pandasDF2.iterrows():
        result[index]= dict(row)
    return result

if __name__ == "__main__":
    app.run(debug=True)
