from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    count, col, when, row_number, desc, expr, countDistinct, sum
)
from pyspark.sql.window import Window
from utils import read_yaml, read_data_from_csv, write_data_to_file

class CarCrashSparkApplication:
    def __init__(self, spark: SparkSession, path_to_config_file: str):
        """
        Initialize the CarCrashSparkApplication by loading datasets from the provided configuration file.
        :param spark: Spark session instance.
        :param path_to_config_file: Path to the YAML configuration file with input file paths.
        """
        input_file_paths = read_yaml(path_to_config_file).get("INPUT_FILE_PATHS")
        self.df_charges = read_data_from_csv(spark, input_file_paths.get("Charges")).distinct()
        self.df_damages = read_data_from_csv(spark, input_file_paths.get("Damages")).distinct()
        self.df_endorse = read_data_from_csv(spark, input_file_paths.get("Endorse")).distinct()
        self.df_primperson = read_data_from_csv(spark, input_file_paths.get("Primary_Person")).distinct()
        self.df_units = read_data_from_csv(spark, input_file_paths.get("Units")).distinct()
        self.df_restrict = read_data_from_csv(spark, input_file_paths.get("Restrict")).distinct()

    def analysis1_count_crash_male_more_than_2(self, output_path: str, output_format: str) -> None:
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2?
        :param output_path: Path to save the output
        :param output_format: Format in which to save the output
        """
        df_an1 = (self.df_primperson
              .filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') == 1))
              .groupBy('CRASH_ID')
              .agg(sum(col("DEATH_CNT")).alias("NO_OF_MALES_KILLED"))
              .filter(col("NO_OF_MALES_KILLED") > 2)
              .agg(count("NO_OF_MALES_KILLED").alias("NO_OF_CRASHES_OF_MALE_DEATH_GT_2")))

        write_data_to_file(df_an1, output_path, output_format)


    def analysis2_count_2_wheeler_accidents(self, output_path: str, output_format: str) -> None:
        """
        How many two wheelers are booked for crashes?
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        df_an2 = (self.df_units
              .filter(col('VEH_BODY_STYL_ID').isin("MOTORCYCLE", "POLICE MOTORCYCLE"))
              .select(countDistinct('VIN').alias('NO_OF_DISTINCT_TWO_WHEELERS')))
        
        write_data_to_file(df_an2, output_path, output_format)


    def analysis3_get_top_vehmake_driver_died_airbags_notdeployed(self, output_path: str, output_format: str) -> None:
        """
        Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        df_an3 = (self.df_primperson
              .join(self.df_units, on=['CRASH_ID', 'UNIT_NBR'], how="inner")
              .filter((col('PRSN_TYPE_ID').contains('DRIVER')) & 
                      (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED') &
                      (self.df_primperson.DEATH_CNT == 1) & 
                      (col('VEH_BODY_STYL_ID').contains('CAR')))
              .groupBy('VEH_MAKE_ID')
              .agg(countDistinct("VIN").alias('COUNT_OF_VEHICLES_PER_VEH_MAKE'))
              .orderBy(col('COUNT_OF_VEHICLES_PER_VEH_MAKE').desc(), col('VEH_MAKE_ID').asc())
              .limit(5))
        
        write_data_to_file(df_an3, output_path, output_format)


    def analysis4_get_no_vehicles_with_license_hit_run(self, output_path: str, output_format: str) -> None:
        """
        Determine number of Vehicles with driver having valid licences involved in hit and run?
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        # Considering VALID DRIVER LICENSES as the ones with class ID in the list->
        # [CLASS A, CLASS B, CLASS C, CLASS M, CLASS A AND M, CLASS B AND M, CLASS C AND M]
        df_an4 = (self.df_primperson
              .join(self.df_units, on=['CRASH_ID', 'UNIT_NBR'], how="inner")
              .filter((col('PRSN_TYPE_ID').contains('DRIVER')) & 
                      (col('DRVR_LIC_CLS_ID').contains('CLASS')) &
                      (col('VEH_HNR_FL') == 'Y'))
              .select(countDistinct("VIN").alias("NO_OF_VEHICLES")))
        
        write_data_to_file(df_an4, output_path, output_format)


    def analysis5_get_state_highest_accident_no_female(self, output_path: str, output_format: str) -> None:
        """
        Which state has highest number of accidents in which females are not involved?
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        # This query is written with assumption that the state in which accident occured 
        # and state of VEH_LIC_STATE_ID is same
        df_crash_with_no_female = (
            self.df_primperson
            .withColumn("NO_OF_FEMALES", when(col("PRSN_GNDR_ID") == "FEMALE", 1).otherwise(0))
            .groupBy("CRASH_ID")
            .agg(sum(col("NO_OF_FEMALES")).alias("NO_OF_FEMALES_PER_CRASH"))
            .filter(col("NO_OF_FEMALES_PER_CRASH") == 0)
            .select("CRASH_ID")
        )

        df_an5 = (
            df_crash_with_no_female
            .join(self.df_units, on="CRASH_ID", how="inner")
            .dropDuplicates(["CRASH_ID"])
            .groupBy("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(1)
        )

        write_data_to_file(df_an5, output_path, output_format)


    def analysis6_get_top_vehicle_make_max_injuries(self, output_path: str, output_format: str) -> None:
        """
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        w = Window.orderBy(col("TOTAL_INJURIES_VEH_MAKEID").desc())

        df_an6 = (self.df_units
              .withColumn('TOTAL_INJURIES', col('TOT_INJRY_CNT') + col('DEATH_CNT'))
              .groupBy("VEH_MAKE_ID")
              .agg(sum("TOTAL_INJURIES").alias('TOTAL_INJURIES_VEH_MAKEID'))
              .withColumn("row", row_number().over(w))
              .filter(col("row").between(3, 5)))

        write_data_to_file(df_an6, output_path, output_format)


    def analysis7_get_top_ethnic_ug_each_body_style(self, output_path: str, output_format: str) -> None:
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())

        df_an7 = (self.df_primperson
              .join(self.df_units, on=['CRASH_ID', 'UNIT_NBR'], how='inner')
              .filter(~col('VEH_BODY_STYL_ID').isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]) &
                      ~col('PRSN_ETHNICITY_ID').isin(["NA", "UNKNOWN"]))
              .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
              .count()
              .withColumn("row", row_number().over(w))
              .filter(col("row") == 1)
              .drop("row", "count"))

        write_data_to_file(df_an7, output_path, output_format)

    def analysis8_get_top_5_zip_codes_crash_with_alcohol(self, output_path: str, output_format: str) -> None:
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols
        as the contributing factor to a crash (Use Driver Zip Code)
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        df_an8 = (
            self.df_units
            .join(self.df_primperson, on=["CRASH_ID","UNIT_NBR"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") |
                col("CONTRIB_FACTR_2_ID").contains("ALCOHOL") |
                col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL")
            )
            .filter(col("VEH_BODY_STYL_ID").like("%CAR%"))
            .dropDuplicates(["CRASH_ID"])
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
            .drop("count")
        )

        write_data_to_file(df_an8, output_path, output_format)


    def analysis9_get_crash_ids_with_no_damage(self, output_path: str, output_format: str) -> None:
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is
        above 4 and car avails Insurance
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        damage_list_gt4=["DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST"]
        df_an9 = (
            self.df_units.alias("u")
            .join(self.df_damages.alias("d"), on=["CRASH_ID"], how="left")
            .filter(
                (col("u.VEH_DMAG_SCL_1_ID").isin(damage_list_gt4))  |
                (col("u.VEH_DMAG_SCL_2_ID").isin(damage_list_gt4))
            )
            .filter((col("d.DAMAGED_PROPERTY") == "NONE") | (col("d.DAMAGED_PROPERTY").isNull()==True))
            .filter(col("u.FIN_RESP_TYPE_ID").like("%INSURANCE%"))
            .filter(col("u.VEH_BODY_STYL_ID").like("%CAR%"))
            .select(countDistinct("u.CRASH_ID").alias("COUNT_OF_DISTINCT_CRASH_IDS"))
        )

        write_data_to_file(df_an9, output_path, output_format)


    def analysis10_get_top_5_vehicle_makes_speed_offences(self, output_path: str, output_format: str) -> None:
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
        with highest number of offences (to be deduced from the data)
        :param output_format: Format in which to save the output
        :param output_path: Path to save the output
        """
        top25_offences_state = [
            row[0] for row in self.df_units
            .join(self.df_charges, on=["CRASH_ID","UNIT_NBR"], how="inner")
            .filter(col("VEH_LIC_STATE_ID").cast("int").isNull())
            .dropDuplicates(["CRASH_ID","UNIT_NBR"])
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]

        top10_vehicle_colors = [
            row[0] for row in self.df_units
            .filter(col("VEH_COLOR_ID") != "NA")
            .dropDuplicates(["CRASH_ID","UNIT_NBR"])
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df_an10 = (
            self.df_charges
            .join(self.df_primperson, on=["CRASH_ID","UNIT_NBR"], how="inner")
            .join(self.df_units, on=["CRASH_ID","UNIT_NBR"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(self.df_primperson.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            .filter(self.df_units.VEH_COLOR_ID.isin(top10_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top25_offences_state))
            .dropDuplicates(["CRASH_ID","UNIT_NBR"])
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
            .drop("count")
        )

        write_data_to_file(df_an10, output_path, output_format)
