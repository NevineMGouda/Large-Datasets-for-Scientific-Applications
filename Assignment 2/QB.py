# Question B.1
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,length,expr,monotonically_increasing_id,sum,abs,count

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark_session = SparkSession.builder.appName("GenderPay").getOrCreate()

    file_path = "UK_Gender_Pay_Gap_Data-2017_to_2018.csv"
    try:
        df = spark_session.read.format("csv").option("multiLine", "true").option("header", "true"). \
            option("inferSchema", "true").option("delimiter", ","). \
            option("ignoreTrailingWhiteSpace", "true").option("ignoreLeadingWhiteSpace", "true"). \
            load(file_path)
        logger.info("__LDSA__: Successfully loaded the data from: " + file_path)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + file_path)
        sys.exit(0)
    df.createOrReplaceTempView("GenderGap")
    # Question B.1.1
    # Highest gap favoring males using DiffMeanHourlyPercent
    print("Highest gap using DiffMeanHourlyPercent:")
    spark_session.sql(
        "SELECT EmployerName, DiffMeanHourlyPercent FROM GenderGap Where Abs(DiffMeanHourlyPercent) =(SELECT MAX(Abs(DiffMeanHourlyPercent)) FROM GenderGap)").show(
        truncate=False)

    # Highest gap favoring males using DiffMedianHourlyPercent
    print("Highest gap using DiffMedianHourlyPercent:")
    spark_session.sql(
        "SELECT EmployerName, DiffMedianHourlyPercent FROM GenderGap Where Abs(DiffMedianHourlyPercent)= (SELECT MAX(Abs(DiffMedianHourlyPercent)) FROM GenderGap)").show(
        truncate=False)

    ##############################################################
    # Lowest gap favoring males using DiffMeanHourlyPercent including 0.0
    print("Lowest gap using DiffMeanHourlyPercent (including 0.0):")
    spark_session.sql(
        "SELECT EmployerName, DiffMeanHourlyPercent FROM GenderGap Where Abs(DiffMeanHourlyPercent) = (SELECT MIN(Abs(DiffMeanHourlyPercent)) FROM GenderGap)").show(
        truncate=False)

    # Lowest gap favoring males using DiffMeanHourlyPercent excluding 0.0
    print("Lowest gap using DiffMeanHourlyPercent (excluding 0.0):")
    spark_session.sql(
        "SELECT EmployerName, DiffMeanHourlyPercent FROM GenderGap Where Abs(DiffMeanHourlyPercent) = (SELECT MIN(Abs(DiffMeanHourlyPercent)) FROM GenderGap WHERE DiffMeanHourlyPercent!= 0.0)"). \
        show(truncate=False)

    ##############################################################
    # Lowest gap favoring males using DiffMedianHourlyPercent including 0.0
    print("Lowest gap using DiffMedianHourlyPercent (including 0.0):")
    spark_session.sql(
        "SELECT EmployerName, DiffMedianHourlyPercent FROM GenderGap Where Abs(DiffMedianHourlyPercent) = (SELECT MIN(Abs(DiffMedianHourlyPercent)) FROM GenderGap)").show(
        truncate=False)

    # Lowest gap favoring males using DiffMedianHourlyPercent excluding 0.0
    print("Lowest gap using DiffMedianHourlyPercent (excluding 0.0):")
    spark_session.sql(
        "SELECT EmployerName, DiffMedianHourlyPercent FROM GenderGap Where Abs(DiffMedianHourlyPercent) = (SELECT MIN(Abs(DiffMedianHourlyPercent)) FROM GenderGap WHERE DiffMedianHourlyPercent!= 0.0)").show(
        truncate=False)
    ##############################################################

    # Question B.1.2
    result = spark_session.sql("SELECT SUM(abs(DiffMeanHourlyPercent))/COUNT(*) as OverallAverage FROM GenderGap")
    ##############################################################
    # Question B.1.3
    result.write.save("OverallMeanGenderPayGap.csv", format="csv", header="true")
    ##############################################################
    # Question B.1.4
    spark_session.sql("SELECT COUNT(*)*100/("
                      "SELECT CAST(COUNT(*) AS float) "
                      "FROM GenderGap"
                      ") as Percentage "
                      "From GenderGap "
                      "Where DiffMeanHourlyPercent < 0").show()
    ##############################################################
    # Question B.2.1

    sector_file_path = "sector.csv"
    try:
        sector_df = spark_session.read.format("csv"). \
            option("multiLine", "true"). \
            option("header", "true"). \
            option("inferSchema", "true"). \
            option("delimiter", ","). \
            option("ignoreTrailingWhiteSpace", "true"). \
            option("ignoreLeadingWhiteSpace", "true"). \
            load(sector_file_path)
        logger.info("__LDSA__: Successfully loaded the data from: " + file_path)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + file_path)
        sys.exit(0)
    sector_df.createOrReplaceTempView("Sector")

    sectorGroupTable = df.filter(split(df.SicCodes, ",")[0] != 1). \
        select(df.EmployerName,
               expr('substring(SicCodes, 1, 1+((length(split(SicCodes, ",")[0])) % 4))').alias("SectorGroup"))

    CompanySectorTable = sectorGroupTable.join(sector_df). \
        where(sectorGroupTable["SectorGroup"].between(sector_df["Min"], sector_df["Max"])). \
        select(sector_df["SectorID"])

    df = df.withColumn("row_index", monotonically_increasing_id())
    CompanySectorTable = CompanySectorTable.withColumn("row_index", monotonically_increasing_id())
    df = df.join(CompanySectorTable, on=["row_index"]).sort("row_index").drop("row_index")
    # df.select("EmployerName", "SectorID").show(truncate=False)
    ###########################################################
    # Question B.2.2
    Sector_gpg = df.groupBy("SectorID"). \
        agg(sum(abs(df["DiffMeanHourlyPercent"])).alias("SectorAverageGenderPayGap")). \
        join(sector_df, on=["SectorID"]). \
        select("SectorID", "Sector", "SectorAverageGenderPayGap")
    Sector_gpg.show(truncate=False)
    ###########################################################

    # Question B.2.3
    Sector_gpg = df.groupBy("SectorID"). \
        agg(sum(abs(df["DiffMeanHourlyPercent"])).alias("SectorAverageGenderPayGap"),
            count("EmployerName").alias("NumberOfCompanies")). \
        join(sector_df, on=["SectorID"]). \
        select("SectorID", "Sector", "SectorAverageGenderPayGap", "NumberOfCompanies")
    Sector_gpg.sort("SectorAverageGenderPayGap").show(truncate=False)
    ###########################################################
if __name__ == "__main__":
    main()
