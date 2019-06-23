from databaseConnection import dataBaseConnect

class createTabels:
    newConection = dataBaseConnect().connectToDataBase()
    print ( newConection.get_dsn_parameters(),"\n")
    """
    create table for station 
    sorucefile: ghcnd-stations.txt
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
    
    newConection.commit()
    cursor.close()
    newConection.close()  
    
    
if __name__ == '__main__':
    createTabels()
