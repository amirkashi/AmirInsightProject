from constantValues.loginInfo import AwsLogins
from databaseConnection import dataBaseConnect


class calculateAnualInterval:
    def createTable(self, itemName, intervalLength):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        tableName = itemName + "_" + str(intervalLength)
        table =  "DROP TABLE IF EXISTS " + tableName + " " +\
                 "CREATE TABLE " + tableName + " ( " +\
                 "id NOT NULL, " +\
                        station_name VARCHAR(20) UNIQUE NOT NULL,
                        latitude real NOT NULL,
                        longitude real NOT NULL,
                        elevation real NOT NULL,
                        state VARCHAR(20) NOT NULL,
                        name VARCHAR(100) NOT NULL
                    );
                    
        cursor.execute(stationTable)
        print("station table created")
        newConection.commit()
        cursor.close()
        newConection.close() 




if __name__ == '__main__':
    intervalLength = 20
    TMAX = "tmax"
    TMIN = "tmin"
    PRCP = "prcp"


