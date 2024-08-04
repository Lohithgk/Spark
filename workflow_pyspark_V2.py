from pyspark.sql import SparkSession, functions as f, dataframe
import os

# Defining Variables
path='/home/lohith/Data/Superstore.csv'
Destination_path = '/home/lohith/Data/transformed_Superstore.csv'

# define a function to check file exists or not
def check_file_exists(path):
    try:
        if os.path.exists(path):
            return path            
        else:
            return None
    except:
        return None

# defining read_csv function
def read_transform(path):
    try:
        # create a spark session
        spark = SparkSession.builder.getOrCreate()
        # read csv file
        df: dataframe = (spark
                        .read
                        .csv(path=path, 
                            header=True, 
                            inferSchema=True)
                        .select('Country', 'State', 'Segment')
                        .filter((f.col('Country') == 'United States') & (f.col('Segment') == 'Consumer')))
        return df
    
    except:
        return None

# function to write dataframe to csv file 
def write_csv(dataframe, path):
    try:
        (dataframe
         .write
         .option('header', 'true')
         .mode('overwrite')
         .format('csv')
         .save(path))
        
        return True
    
    except:
        return None

# Call main function
if __name__ == "__main__":
    
    # calling check_file_exists function, return value will be stored in file_path
    file_path = check_file_exists(path)
    # if return path is None, then file not found, and exit the program.
    if file_path is None:
        print("File not found")
        exit()
    
    # calling read_transform function, return value will be stored in transformed_df
    transformed_df = read_transform(path=file_path)
    # check read_transform is successful or not, if not then exit
    if transformed_df is None:
        print("No file to read data")
        exit()

    # calling write_csv function, return value will be stored in write_csv_status
    write_csv_status = write_csv(transformed_df, Destination_path)
    # check if file is written successful or not, if not then exit
    if write_csv_status is None:
        print("fail to write file")
        exit()
    else:
        print("File written successfully.")
        exit()