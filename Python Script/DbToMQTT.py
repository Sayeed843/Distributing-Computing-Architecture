import paho.mqtt.client as mqtt
import sys
import pymysql as pymysql
from time import sleep


class DBoperation():


    def __init__(self, user='root', password='12345678', host='127.0.0.1',port=3307, dbName='restapps'):
        self.user=user
        self.password=password
        self.host=host
        self.port=port
        self.dbName=dbName


    def dbConnection(self):
        try:
            self.db=pymysql.connect(user=self.user, password=self.password,host=self.host, port=self.port, db=self.dbName)

        except pymysql.Error as e:
            print(e)
            print("Could Not connect to the database")
            print("Close")
            sys.exit()
        self.cursor=self.db.cursor()

    def executeData(self, sql, args):
        try:
            self.cursor.execute(sql, args)
            self.db.commit()

        except pymymql.Error as e:
            self.db.rollback()
            print(e)
            print("Execute Data into Database.......Failed")

    def getCursor(self):
        cursorList = []
        # print(str(self.cursor.fetchall()))
        for cur in self.cursor.fetchall():
            cursorList.append(cur)
        return cursorList

    def returnCursor(self):
        cursorList = []
        for cur in self.cursor.fetchall():
            cursorList.append(cur)
        return cursorList

    def display(self):
        for element in self.cursor:
            print(element[0])


class Sensor():
    db = DBoperation()

    def insertSensorTable(self, value,topic):
        sql = "INSERT INTO `Sensor`.`SensorValue`\
        (`sensor`,`data`,`SensorValue`.`time`) \
        VALUES(%s,%s,CURRENT_TIMESTAMP)"

        args = (topic,value)
        self.db.dbConnection()
        self.db.executeData(sql, args)

        # Agent Function

    def rSensor(self):      # retrieve Sensor table data
        sql = "SELECT `actuator`.`atag` ,\
        `actuator`.`action` \
        FROM `restapps`.`actuator` \
        ORDER BY `actuator`.`time` DESC LIMIT 1;"
        args = ()
        self.db.dbConnection()
        self.db.executeData(sql, args)
        return (self.db.getCursor())



class MqttBroker():
    s = Sensor()
    db = DBoperation()

    def __init__(self, server="m10.cloudmqtt.com", port=17635,
                 username="ewpjantq", password="Rfnyu4oiB8WO"):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.client = mqtt.Client()

    def mqttConnection(self):
        try:
            self.client.username_pw_set(self.username, self.password)
            self.client.connect(self.server, self.port)
            # print("Connection with MQTT")
        except self.client.Error as e:
            print(e)
            print("Could not connect to the MQTT broker....")
            print("Closing....")
            sys.exit()

    def on_connect(self, client, userdata, flags, rc):
        print("Connedted -Result code: " + str(rc))


    def on_message(self, client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))


    def mqttLoopForever(self):
        self.client.on_connect = self.on_connect
        data = self.s.rSensor()
        # print(data)


        for d in data:
            # print("Data: "+d)
            # decision = self.s.rSensor(str(d))
            print("Actuator Tag: " + str(d[0]) + "    Action:" + str(d[1]))
            self.client.publish(str(d[0]), str(d[1]))
            # if (decision[0] == 1):
            #     print("Ami Aci" + str(decision))
            #     self.client.publish(str(d), str(decision[0]))
            sleep(1.0)
            self.mqttLoopForever()

            try:
                self.client.loop_forever()
                print("Okay Man")
            except KeyboardInterrupt:
                print("Closing...")



broker = MqttBroker()
# database.dbConnection()
broker.mqttConnection()


broker.mqttLoopForever()
