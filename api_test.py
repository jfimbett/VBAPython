import requests

data = {"a": 5, "b": 7}

response = requests.post("http://127.0.0.1:5000/api/sum", json=data)
print(response.json())