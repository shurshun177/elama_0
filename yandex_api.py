import requests
url = 'https://api.direct.yandex.com/json/v5/campaigns'

r=requests.get(url,headers={"Authorization":'Bearer ****************************','Client-Login':'*********'},
               json = {'method':'get','params':{'SelectionCriteria':{},"FieldNames":["Id", "Name"]}})
q=r.headers
d=r.json()
print (d)
