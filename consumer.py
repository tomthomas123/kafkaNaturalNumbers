import mysql.connector
from kafka import KafkaConsumer

try:
    mydb = mysql.connector.connect(host = 'localhost', user = 'root', password = '',database = 'kafkaevendb')
except mysql.connector.Error as e:
    print("MySql error",e)

mycursor = mydb.cursor()

bootstrap_server = ["localhost:9092"]

topic = "naturalnumbers"

consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_server)

for i in consumer:
    print(str(i.value.decode()))
    random_even = int(i.value.decode())
    if(random_even % 2)==0:
        sql = "INSERT INTO `evennumbers`( `evennumbers`) VALUES (%s)"
        data = (random_even,)
        mycursor.execute(sql,data)
        mydb.commit()
        print("Even number added to db ", random_even)