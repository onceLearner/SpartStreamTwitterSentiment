import socket
import time
import requests

PORT = 9993
url = "https://api.twitter.com/2/tweets/search/recent?query=movie%20batman&max_results=10"
header = {
    "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAAN6TYAEAAAAA1xhR02IxMt6bY50M4d3eopUlitU%3DRrIWdlU9pCwNEt0uf6DOvP93iOPeVdolyeF5KUDvhRPj3yQPSU"}


def get_tweets():
    response = requests.get(url, headers=header)
    tweet_text = response.json()["data"][0]["text"]
    return tweet_text

# create a socket object
serversocket = socket.socket(
    socket.AF_INET, socket.SOCK_STREAM)

# get local machine name
host = socket.gethostname()

# bind to the port
serversocket.bind((host, PORT))

# queue up to 5 requests
serversocket.listen(5)

print("----SERVER LISTENING------ " + str(PORT))

# establish a connection
clientsocket, addr = serversocket.accept()

print("Got a connection from %s" % str(addr))

while True:
    text=get_tweets()
    # text = "hello hello " + str(randrange(100))
    print(text)
    clientsocket.send(bytes("{}\n".format(text), "utf-8"))
    time.sleep(4)




