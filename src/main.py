import os
from analysis import CarCrashSparkApplication
from utils import read_yaml, read_data_from_csv, write_data_to_file
from pyspark.sql import SparkSession

def main():
    """
    Main entry point for the Car Crash Spark Application.
    Initializes the Spark session, reads the configuration file,
    and executes various analyses.
    """
    # Initialize sparks session
    spark = SparkSession \
            .builder \
            .appName("CarCrash_Spark_Application") \
            .getOrCreate()
    
    # Configuration file path
    config_file_path = os.path.abspath("configs/config.yaml")

    # Create analysis object
    analysis_object = CarCrashSparkApplication(spark, config_file_path)

    # Load output paths and file format from configuration
    try:
        config = read_yaml(config_file_path)
        output_file_paths = config.get("OUTPUT_PATHS", {})
        file_format = config.get("FILE_FORMAT", {}).get("Output")
    except Exception as e:
        print(f"Error reading config file: {e}")
        spark.stop()
        return


    # 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
    analysis_object.analysis1_count_crash_male_more_than_2(output_file_paths.get('output_path_1'), file_format)

    # 2: How many two wheelers are booked for crashes?
    analysis_object.analysis2_count_2_wheeler_accidents(output_file_paths.get('output_path_2'), file_format)

    # 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died
    # and Airbags did not deploy.
    analysis_object.analysis3_get_top_vehmake_driver_died_airbags_notdeployed(output_file_paths.get('output_path_3'),file_format)

    # 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
    analysis_object.analysis4_get_no_vehicles_with_license_hit_run(output_file_paths.get('output_path_4'), file_format)

    # 5: Which state has highest number of accidents in which females are not involved?
    analysis_object.analysis5_get_state_highest_accident_no_female(output_file_paths.get('output_path_5'), file_format)

    # 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    analysis_object.analysis6_get_top_vehicle_make_max_injuries(output_file_paths.get('output_path_6'),
                                                                                file_format)

    # 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    analysis_object.analysis7_get_top_ethnic_ug_each_body_style(output_file_paths.get('output_path_7'), file_format)

    # 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as
    # the contributing factor to a crash (Use Driver Zip Code)
    analysis_object.analysis8_get_top_5_zip_codes_crash_with_alcohol(output_file_paths.get('output_path_8'), file_format)

    # 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~)
    # is above 4 and car avails Insurance
    analysis_object.analysis9_get_crash_ids_with_no_damage(output_file_paths.get('output_path_9'), file_format)

    # 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
    # has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with
    # highest number of offences (to be deduced from the data)
    analysis_object.analysis10_get_top_5_vehicle_makes_speed_offences(output_file_paths.get('output_path_10'), file_format)

    spark.stop()

if __name__ == '__main__':
    main()