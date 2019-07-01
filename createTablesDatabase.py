from databaseConnection import dataBaseConnect

class createTabels:
    newConection = dataBaseConnect().connectToDataBase()
    cursor = newConection.cursor() 

    climateDataTableDaily = '''
                       DROP TABLE IF EXISTS stations_data_daily;
                       CREATE TABLE stations_data_daily(
                            noaa_id VARCHAR(20) NOT NULL,
                            year INTEGER NOT NULL,
                            month INTEGER NOT NULL,
                            day INTEGER NOT NULL,
                            precipitation real,
                            snowfall real,
                            snow_depth real,
                            max_temperature real,
                            min_temperature real
                       );
                       '''
    cursor.execute(climateDataTableDaily)
    print("daily caimate table is created")


    climateDataTableMontly = '''
                       DROP TABLE IF EXISTS stations_data_montly;
                       CREATE TABLE stations_data_montly(
                            noaa_id VARCHAR(20) NOT NULL,
                            year INTEGER NOT NULL,
                            month INTEGER NOT NULL,
                            precipitation real,
                            snowfall real,
                            snow_depth real,
                            max_temperature real,
                            min_temperature real
                       );
                       '''
    cursor.execute(climateDataTableMontly)
    print("daily caimate table is created")

    newConection.commit()
    cursor.close()
    newConection.close()  
    
    
if __name__ == '__main__':
    createTabels()
