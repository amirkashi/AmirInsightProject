import psycopg2 as pg2
from constantValues.loginInfo import dataBaseLogin


class dataBaseConnect:
    def connectToDataBase(self):
        """
        This function provides connection to database
        and will be used by several classes
        """
        connection = None
        try:
            loginInfo = dataBaseLogin()
            connection = pg2.connect(user = loginInfo.user,
                                     password = loginInfo.password,
                                     host = loginInfo.hostIp,
                                     port = loginInfo.port,
                                     database = loginInfo.database)

            print ("Connected to Database", "\n")
        except (Exception, pg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)
        return connection


