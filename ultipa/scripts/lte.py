from ultipa import ULTIPA_REQUEST,UltipaConfig,Connection
ultipaConfig = UltipaConfig()
ultipaConfig.hosts = ["127.0.0.1:60061"]
ultipaConfig.username = "ldbc"
ultipaConfig.password = "ldbc"
conn = Connection.NewConnection(defaultConfig=ultipaConfig)
conn.defaultConfig.setDefaultGraphName("sf1")

uql1="LTE().node_property(@{Transfer}.`{amount}`)"
conn.uql(uql1)
print(uql1)

uql2="LTE().node_property(@{Guarantee}.`{amount}`)"
conn.uql(uql2)
print(uql2)