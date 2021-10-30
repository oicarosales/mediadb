#!/usr/bin/python3
# -*- coding: utf-8 -*-

import mysql.connector                          # pip install mysql-connector-python
from setup import host, user, passwd, database  # cire arquivo setup.py com cada dado em uma variavel
import glob
from datetime import datetime
import threading

# CRIA CONEXAO COM O BANCO DE DADOS
MYDB = mysql.connector.connect(
    host=host,
    user=user,
    passwd=passwd,
    database=database
)
MYCURSOR = MYDB.cursor()

try:
    # CRIA TABELA MP3_FILES CASO A TABELA NÃO EXISTA
    MYCURSOR.execute("CREATE TABLE IF NOT EXISTS mp3_files (id INT AUTO_INCREMENT PRIMARY KEY, file LONGBLOB, file_path VARCHAR(100), date_insert DATETIME)")
    MYDB.commit()
except mysql.connector.Error as err:
    print("Erro ao criar tabela: {}".format(err))
    exit(1)

# LISTA TODOS OS ARQUIVOS MP3 NO DIRETORIO
digital_path = glob.glob("/home/icaro/Music/code/*.mp3")  #SUBSTITUA PELO CAMINHO DO SEU DIRETORIO DE AUDIOS
   

# FUNÇAO PARA CONVERTER O ARQUIVO EM BINARIO
def convert_into_binary(file):
    with open(file, 'rb') as f:
        audio = f.read()
    return audio
        
        
# FUNCAO PARA INSERIR OS ARQUIVOS NO BANCO DE DADOS
def insert_into_database(audio):
    SQL = "INSERT INTO mp3_files (file, file_path, date_insert) VALUES (%s, %s, %s)"
    for file in digital_path:
        file_path = file
        MYCURSOR.execute("set global net_buffer_length=1000000")
        MYCURSOR.execute("set global max_allowed_packet=1000000000")
        MYCURSOR.execute(SQL, (convert_into_binary(file), file_path, datetime.now()))
        MYDB.commit()
    MYDB.close()
    
    
# EXECUTA A FUNCAO INSERT_INTO_DATABASE USANDO THREADS
try:
    threading.Thread (target=insert_into_database, args=(convert_into_binary(digital_path[0]),)).start()
except mysql.connector.Error as err:
    print("Erro ao inserir no banco de dados: {}".format(err))
    exit(1)
