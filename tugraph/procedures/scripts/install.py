import sys
import requests
import json
import base64

if len(sys.argv) < 4:
    print('usage: %s [endpoint] [plugin_name] [plugin_type]' % sys.argv[0])
    print('[endpoint] should be in the format of [address:port]')
    print('[plugin_type] can be RO (for read-only) or RW (read-write)')
    sys.exit()

endpoint = sys.argv[1] # addr:port
plugin_name = sys.argv[2]
read_only = (sys.argv[3] == 'RO')

r = requests.post(url='http://%s/login' % endpoint, data=json.dumps({'user':'admin', 'password':'73@TuGraph'}), headers={'Content-Type':'application/json'})
jwt = r.json()['jwt']

r = requests.delete(url='http://%s/db/default/cpp_plugin/%s' % (endpoint, plugin_name), headers={'Authorization':'Bearer %s' % jwt})
print(r.status_code)
print(r.content)

data = {'name':'%s' % plugin_name}
content = open('%s.so' % plugin_name, 'rb').read()
data['code_base64'] = base64.b64encode(content).decode()
data['description'] = '%s' % plugin_name
data['read_only'] = read_only
js = json.dumps(data)
r = requests.post(url='http://%s/db/default/cpp_plugin' % endpoint, data=js, headers={'Content-Type':'application/json', 'Authorization':'Bearer %s' % jwt})
print(r.status_code)
print(r.content)
