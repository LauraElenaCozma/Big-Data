import csv
import socket
import time
import json

HOST = "localhost"  # Standard loopback interface address (localhost)
PORT = 5554  # Port to listen on (non-privileged ports are > 1023)
CSV_FILE_PATH = 'hr_test.csv'


def csv_to_json(csvFilePath):
    jsonArray = []

    # read csv file
    with open(csvFilePath, encoding='utf-8') as csvf:
        # load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)

        # convert each csv row into python dict
        for row in csvReader:
            jsonArray.append(row)

        return jsonArray


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()

    data_array = ['a ab sad adas das sa '] #csv_to_json(CSV_FILE_PATH)

    length = 15
    total_length = len(data_array)
    start = 0
    stop = length + start

    conn, addr = s.accept()
    with conn:
        print(f"Connected by {addr}")

        while True:
            print('running\n')
            data = data_array[start:stop]
            # data_send = [json.dumps(obj).encode() for obj in data_array]
            data_send = 'a b c d e f g'.encode('utf-8')
            start = stop
            stop = start + length
            time.sleep(1)
            print('TO SEND\n')
            print(data_send)
            bytearray(data_send)
            conn.send(bytearray(data_send))
