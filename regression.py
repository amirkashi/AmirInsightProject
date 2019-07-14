import numpy as np
from sklearn.linear_model import LinearRegression
from databaseConnection import dataBaseConnect
import psycopg2 as ps2
from pandas import DataFrame
import pandas as pd


class calculateRegression:
    def getAnnualData(self, climateItem, startYear, endYear):
        query = """
                SELECT * FROM  (SELECT id, elevation  ,AVG({}) AS item 
                    FROM lower48_data_elv 
                    WHERE year BETWEEN {} AND {} 
                    GROUP BY id,  elevation) 
                    tempTable WHERE tempTable.item IS NOT NULL;
                """.format(climateItem, startYear, endYear)
        return self.makeDataframeFromQuery(query)


    def makeDataframeFromQuery(self, query):
        newConection = dataBaseConnect().connectToDataBase()
        dataframe = pd.read_sql(query, newConection)
        newConection.close()
        return dataframe

    def updateDataBaseAnnually(self, tableName, columnName, slope, intercept):
        newConection = dataBaseConnect().connectToDataBase()
        cursor = newConection.cursor()
        query = "update " + tableName + " " + \
                "set " + columnName + "  = elevation * %s + %s ;"
        cursor.execute(query, (slope, intercept))
        newConection.commit()
        cursor.close()
        newConection.close()


    def calculateAnnulCoefficients(self,tableName, climateItem):
        startYears = [1919, 1939, 1959, 1979, 1999]
        endYears = [1938, 1958, 1978, 1998, 2018]
        for i in range(len(startYears)):
            columnName =  "from_{}_{}".format(startYears[i], endYears[i])
            dataFrame = calculateAnnualRegression.getAnnualData(climateItem, startYears[i], endYears[i])
            elevation = np.array(dataFrame.elevation.tolist()).reshape((-1, 1))
            data = dataFrame.item.tolist()
            model = LinearRegression().fit(elevation, data)
            r_sq = model.score(elevation, data)
            self.updateDataBaseAnnually(tableName, columnName, model.coef_[0], model.intercept_)
            print('Coefficient of determination:', r_sq)
            print('Intercept:', model.intercept_)
            print('Slope:', model.coef_[0])



if __name__ == '__main__':
    calculateAnnualRegression = calculateRegression()
    calculateAnnualRegression.calculateAnnulCoefficients("tmax_20_year", "tmax")
    calculateAnnualRegression.calculateAnnulCoefficients("tmin_20_year", "tmin")
    calculateAnnualRegression.calculateAnnulCoefficients("prcp_20_year", "prcp")


