import jaydebeapi as jp


def test_db_connection():
    pass


if __name__ == '__main__':
    ## bimatrix server setting
    # conn = jp.connect(
    #     'com.tmax.tibero.jdbc.TbDriver',  # classname
    #     'jdbc:tibero:thin:@192.168.11.126:8629:tibero',
    #     {'user': 'sys', 'password': 'matrix'},
    #     './linux/lib/tibero6-jdbc.jar'
    # )

    ## mois server setting
    # conn = jp.connect(
    #     'com.tmax.tibero.jdbc.TbDriver',  # SAME
    #     'jdbc:tibero:thin:@10.47.61.5:1579:DKRMS',
    #     {'user': 'DRM', 'password': 'krms51mrd'},
    #     './linux/lib/tibero6-jdbc.jar'
    # )

    ## properties setting
    from m4.ApplicationConfiguration import ApplicationConfiguration
    config: ApplicationConfiguration = ApplicationConfiguration.instance()
    config.init('m4.properties')
    config.parsing_properties()

    conn = jp.connect(
        config.parameter("ds.connection.jclassnm"),
        config.parameter("ds.connection.url"),
        {'user': config.parameter("ds.connection.user"), 'password': config.parameter("ds.connection.user")},
        config.parameter('ds.connection.jdbcpath')
    )

    with conn.cursor() as curs:
        sql = """SELECT ID, NAME FROM TIBERO.TMP"""
        curs.execute(sql)
        columns = list(map(lambda x: x[0], curs.description))
        rows = curs.fetchall()

        print(columns)
        for row in rows:
            print(row)
    conn.close()
