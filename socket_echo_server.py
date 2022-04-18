
from ast import If
from itertools import count
from pickle import NONE
import socket
import sys
import  python_to_postgres
from datetime import datetime
#from prueba import prueba
import uuid
import psycopg2
import time

hostname = '127.0.0.1'
DATABASE = 'pluvio'
username = 'postgres'
PWD = 'postgres'
port_id = 5432
conn = None
cur = None 

try:
 conn = psycopg2.connect(

    host = hostname,
    dbname = DATABASE,
    user = username,
    password = PWD,
    port = port_id
    )

 print("conexion exitosa") 
 cursor = conn.cursor()
 cursor.execute("select version()")
 row = cursor.fetchone()
 print(row)
 #cursor.execute("select * from ws_weather_station where wstation_passkey = :passkey")
 #rows = cursor.fetchall()
 #for row in rows:
  #   print(row)
except Exception as e:
        print(e)


 # Create a TCP/IP socket
sock = socket.socket()

# Bind the socket to the port
server_address = ('192.168.0.11', 5000)
print('starting up on {} port {}'.format(*server_address))
sock.bind(server_address)

# Listen for incoming connections
sock.listen(5)

def my_random_string(string_length=10):
 """"Returns a random string of length string_length."""
 random = str(uuid.uuid4()) # Convert UUID format to a python string. 
 random = random.upper() # Make all characters uppercase.
 random = random.replace("-","") # Remove the UUID '-'. 
 return random[0:string_length] # Return the random string.

while True:
    # Wait for a connection
    print('waiting for a connection')
    active, client_address = sock.accept()
    try:
        #print('cliente conectado', client_address)

        # Receive the data in small chunks and retransmit it
        while True:
         data = active.recv(1024)
         #print('received {!r}'.format(data))
         if data:
            # print('data del client',data)
             stringdata = data.decode('utf-8') # se convierte en string
            # print(stringdata)
             clean_data = stringdata.split('\r\n\r\n')[1]

             if "PASSKEY" in clean_data and "GW1100B" in clean_data:
                datDic = dict(item.split("=") for item in clean_data.split("&"))
                print('la data limpia ',datDic)
                print('el PASSKEY',datDic['PASSKEY'])
                
                sql="""select * from ws_weather_station where wstation_pa = %s"""
                cursor.execute(sql,(datDic['PASSKEY'],))
                rows = cursor.fetchall()
                for row in rows:
                    print('estacion consultada',row)
                    gpm_code =row[0]
                    print('estacion consultada',gpm_code)
                    wspask = row[15]
                if wspask == datDic['PASSKEY']:
                        print( row[15])
                        for cannel in range(0,9,1):
                           if "temp{}f".format(cannel) in datDic  and "humidity{}".format(cannel) in datDic:
                                print('codigo de estacion consultada',gpm_code)
                                print('canal',cannel)
                                typesensor = 1
                                consensor = """SELECT * FROM exs_sensor WHERE gpm_code = %s and exav_id = %s and exss_channe = %s"""
                                datos = (gpm_code,typesensor,cannel)
                                cursor.execute(consensor,datos) 
                                resl= cursor.fetchall()
                                for sen in resl:
                                 exss_code = sen[3]
                                 print('sensor consultados en base de datos',exss_code) 
                                

                                tempf= datDic["temp{}f".format(cannel)]
                                humed=datDic["humidity{}".format(cannel)]
                                nw = datetime.now()
                                date_tim = nw.strftime("%Y-%m-%d, %H:%M:%S ") 
                                 #================= INSERTAR DATOS ==============
                                #insertdata_sensor= 'INSERT INTO exs_data(exss_code, exdat_temp_1, exdat_humid1, exdat_repor, exdat_creat, exdat_updat) VALUES (%s,%s,%s,%s,%s,%s)'
                                #datosinsert = (exss_code,tempf,humed,date_tim, date_tim, date_tim)
                                #print('datos insertados a la tabla',datosinsert)
                                #cursor.execute(insertdata_sensor,datosinsert)
                                #conn.commit()
                                #count = cursor.rowcount
                        
                                if resl ==[]:
                                       coderand = my_random_string(8)
                                       print('codigo generado para sensor de temp y humed',coderand)
                                       model = string = "WH51"
                                       namesenr = string = "Temp Hum Sensor Channel #{}".format(cannel)  
                                       nw = datetime.now()
                                       date_tim = nw.strftime("%Y-%m-%d, %H:%M:%S ")   
                                    #================= INSERTAR DATOS ==============  
                                       cerate_sensor = 'INSERT INTO exs_sensor(gpm_code, exav_id,exss_code,exmod_model, exss_name, exss_channe, exss_create, exss_update) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                                       datoscrea = (gpm_code,typesensor,coderand,model,namesenr,cannel, date_tim, date_tim)
                                       print('datos insertados a la tabla',datoscrea)
                                       cursor.execute(cerate_sensor,datoscrea)
                                       conn.commit()
                                       count = cursor.rowcount
                                       
                                       if resl is not None:

                                          print('canal',cannel)
                                          typesensor = 1
                                          consens = """SELECT * FROM exs_sensor WHERE gpm_code = %s and exav_id = %s and exss_channe = %s"""
                                          dato = (gpm_code,typesensor,cannel)
                                          cursor.execute(consens,dato) 
                                          resl= cursor.fetchall() 
                                          for cre in resl:
                                           exss_cod =cre[3]                                          
                                          print('el codigo para insertar despues de creado',exss_cod)
                                          print('insertando depues de crear sensor')
                                          nw = datetime.now()
                                          date_tim = nw.strftime("%Y-%m-%d, %H:%M:%S ") 
                                          tempf= datDic["temp{}f".format(cannel)]
                                          humed=datDic["humidity{}".format(cannel)]

                                          #================= INSERTAR DATOS DESPUES CREADO EL SENSOR==============
                                          insertdata_sensor= 'INSERT INTO exs_data(exss_code, exdat_temp_1, exdat_humid1, exdat_repor, exdat_creat, exdat_updat) VALUES (%s,%s,%s,%s,%s,%s)'
                                          datosinsert = (exss_cod,tempf,humed,date_tim, date_tim, date_tim)
                                          print('datos insertados a la tabla despues de creado el sensor de temp y humed',datosinsert)
                                          cursor.execute(insertdata_sensor,datosinsert)
                                          conn.commit()
                                          count = cursor.rowcount
                                else:
                                     print('si esta creado inserta datos')
                                     exss_code = sen[3]
                                      #================= INSERTAR DATOS ==============
                                     insertdata_sensor= 'INSERT INTO exs_data(exss_code, exdat_temp_1, exdat_humid1, exdat_repor, exdat_creat, exdat_updat) VALUES (%s,%s,%s,%s,%s,%s)'
                                     datosinsert = (exss_code,tempf,humed,date_tim, date_tim, date_tim)
                                     print('datos insertados a la tabla',datosinsert)
                                     cursor.execute(insertdata_sensor,datosinsert)
                                     conn.commit()
                                     count = cursor.rowcount

                  #=========================================================
                           #SENSORES DE DETECTOR DE TORMENTA CANALES
                                
                if "lightning_time" in datDic and "lightning_num" in datDic and "lightning" in datDic:
                  print('codigo de estacion consultada PARA EL SENSOR DE RAYO',gpm_code)
                  typesensor = 7
                  channel = 0
                  senray = """SELECT * FROM exs_sensor WHERE gpm_code = %s and exav_id = %s and exss_channe = %s"""
                  raydata =(gpm_code,typesensor,channel)
                  cursor.execute(senray,raydata)
                  rayresl = cursor.fetchall()
                  print('detetor de rayos consultado',rayresl)
                  for ray in rayresl:
                     exs_cod = [3]


                  if rayresl == []:
                                          
                     coderand = my_random_string(8)
                     print('codigo generado para sensor de rayos',coderand)
                     nw = datetime.now()
                     date_tim = nw.strftime("%Y-%m-%d, %H:%M:%S ") 
                     modelo= string = 'WH57'
                     namesenr = string = "Lightning #{}".format(channel) 
                     cerate_rayo = 'INSERT INTO exs_sensor(gpm_code, exav_id,exss_code, exmod_model, exss_name, exss_channe, exss_create, exss_update) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                     datosray = (gpm_code,typesensor,coderand,modelo,namesenr,channel, date_tim, date_tim) 
                     print('CREANDO sensor',datosray)
                     cursor.execute(cerate_rayo,datosray)
                     conn.commit()
                     count = cursor.rowcount 
                     if rayresl is not None:
                        print('datos insertados a la tabla despues de creado el sensor de rayos')
                        typesensor = 7
                        channel = 0
                        senra = """SELECT * FROM exs_sensor WHERE gpm_code = %s and exav_id = %s and exss_channe = %s"""
                        raydat =(gpm_code,typesensor,channel)
                        cursor.execute(senra,raydat)
                        raresl = cursor.fetchall()
                        for rays in raresl:
                           exs_code = rays[3]

                     epoch_time= int(datDic["lightning_time"])
                     exdat_repor =  str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_time)))
                     print('fecha convertida de numero a formatos fecha larga', exdat_repor)
                     nw = datetime.now()
                     date_tim = nw.strftime("%Y-%m-%d, %H:%M:%S ") 
                     exdat_light = datDic['lightning']
                     #================= INSERTAR DATOS ==============
                     insedat_senso= 'INSERT INTO exs_data(exss_code, exdat_light, exdat_repor, exdat_creat, exdat_updat) VALUES (%s,%s,%s,%s,%s)'
                     datinse = (exs_code,exdat_light,exdat_repor, date_tim, date_tim)
                     print('datos insertados a la tabla',datosinsert)
                     cursor.execute(insedat_senso,datinse)
                     conn.commit()
                     count = cursor.rowcount
                  else:
                     print('voy aqui')
                     exs_cod = ray[3]
                     epoch_time= int(datDic["lightning_time"])
                     exdat_repor = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_time)))
                     print('fecha convertida de numero a formatos fecha larga', epoch_time)
                     nw = datetime.now()
                     date_tim = nw.strftime("%Y-%m-%d, %H:%M:%S ") 
                     exdat_light = datDic['lightning']
                     #================= INSERTAR DATOS ==============
                     insedata_sensor= 'INSERT INTO exs_data(exss_code, exdat_light, exdat_repor, exdat_creat, exdat_updat) VALUES (%s,%s,%s,%s,%s)'
                     datoinse = (exs_cod,exdat_light,exdat_repor, date_tim, date_tim)
                     print('datos insertados a la tabla',datosinsert)
                     cursor.execute(insedata_sensor,datoinse)
                     conn.commit()
                     count = cursor.rowcount

             elif  wspask != datDic['PASSKEY']:
                    
                    print('el PASSKEY no esta configurado',datDic['PASSKEY'])
                    active.sendall(data)
                    #break
         else:
            print('cliente', client_address)
            break


                
         
    except psycopg2.InterfaceError as exc:
        
         conn = psycopg2.connect(

         host = hostname,
         dbname = DATABASE,
         user = username,
         password = PWD,
         port = port_id
         )
         cursor = conn.cursor()