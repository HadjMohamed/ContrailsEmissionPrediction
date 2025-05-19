import requests

url = "https://immat.aviation-civile.gouv.fr/immat/servlet/static/upload/export.csv"
r = requests.head(url)

print(r.headers.get("Last-Modified"))
