from databaseInfo import dataBaseLogin


class dataBaseConnect:
    def connectToDataBase(self):
        connection = None
        try:
            loginInfo = dataBaseLogin()
            connection = pg2.connect(user = loginInfo.user,
                                     password = loginInfo.password,
                                     host = loginInfo.hostIp,
                                     port = loginInfo.port,
                                     database = loginInfo.database)
            cursor = connection.cursor()

            print ( connection.get_dsn_parameters(),"\n")
            cursor.execute("SELECT version();")
            print("You are connected to - ", record,"\n")
        except (Exception, pg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)
        return connection
