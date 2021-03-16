import yaml
from flask import Flask, Response, request
from flask_httpauth import HTTPTokenAuth

app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

tokens = []


@auth.verify_token
def verify_token(token):
    return token in tokens


@app.route('/minio/events', methods=['POST'])
@auth.login_required
def index():
    request_json = request.json
    print(request_json['EventName'], request_json['Key'])

    return Response(status=200)


if __name__ == '__main__':
    with open("entity_store/entity_store_config.yaml", 'r') as file:
        try:
            config = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc
    tokens = config['webhook']['tokens']
    app.run(host='0.0.0.0', port=12346)
