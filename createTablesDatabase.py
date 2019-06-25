from databaseConnection import dataBaseConnect

class createTabels:
    newConection = dataBaseConnect().connectToDataBase()
    #print ( newConection.get_dsn_parameters(),"\n")
    """
    create table for station 
    soruce file: ghcnd-stations.txt
    """
    cursor = newConection.cursor()
    print("connected!")
    stationTable = '''
                    DROP TABLE IF EXISTS stations;
                    CREATE TABLE stations (
                        id serial PRIMARY KEY,
                        noaa_id VARCHAR(20) UNIQUE NOT NULL,
                        latitude real NOT NULL,
                        longitude real NOT NULL,
                        elevation real NOT NULL,
                        state VARCHAR(20) NOT NULL,
                        name VARCHAR(100) NOT NULL
                    );
                    '''
    
    cursor.execute(stationTable)
    print("station table created")
    
    climateDataTableDaily = '''
                       DROP TABLE IF EXISTS stations_data_daily;
                       CREATE TABLE stations_data_daily(
                            noaa_id VARCHAR(20) NOT NULL,
                            year INTEGER NOT NULL,
                            month INTEGER NOT NULL,
                            day INTEGER NOT NULL,
                            srecipitation real;
                            snowfall real;
                            snow_depth real;
                            max_temperature real;
                            min_temperature real
                       )
                       '''
    cursor.execute(climateDataTableDaily)

    climateDataTableMontly = '''
                       DROP TABLE IF EXISTS stations_data_montly;
                       CREATE TABLE stations_data_daily(
                            noaa_id VARCHAR(20) NOT NULL,
                            year INTEGER NOT NULL,
                            month INTEGER NOT NULL,
                            srecipitation real;
                            snowfall real;
                            snow_depth real;
                            max_temperature real;
                            min_temperature real
                       )
                       '''
    cursor.execute(climateDataTableMontly)
    
    newConection.commit()
    cursor.close()
    newConection.close()  
    
    
if __name__ == '__main__':
    createTabels()
