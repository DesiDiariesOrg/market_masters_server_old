import json
from datetime import datetime

from flask import Flask, request
from flask_socketio import SocketIO, ConnectionRefusedError
from pymongo import MongoClient, DESCENDING, ASCENDING

# Replace to your MongoDB access string
uri = ("access_string")

client = MongoClient(uri)
db = client.last_quote

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="http://127.0.0.1:5000")  # Replace to your origin


@socketio.on('connect')
def handle_connect():
    # print('Client connected')
    pass


@socketio.on('disconnect')
def handle_disconnect():
    # print('Client disconnected')
    pass


@socketio.on('last_quote_array')
def handle_last_quote_array(data):
    """
        Handle the 'last_quote_array' WebSocket event by querying and emitting data.

        Args:
            data (dict): A dictionary containing query parameters, including 'period' for historical data and 'query' for
                         specifying query conditions.
                        Example data (dict)

        Emits:
            - 'last_quote_array_response_event': Emits JSON data containing the query result.

        """
    try:
        last_quote_array = db.last_quote_array
        if data.get('period'):
            # Historical data query
            start_timestamp = datetime.strptime(data['period']['start'], "%Y-%m-%d").timestamp()
            end_timestamp = datetime.strptime(data['period']['end'], "%Y-%m-%d").timestamp()
            query = {
                "InstrumentIdentifier": data['query']['InstrumentIdentifier'],
                "LastTradeTime": {"$gte": int(start_timestamp), "$lte": int(end_timestamp)}
            }
            count = last_quote_array.count_documents(query)
            if count == 0:
                socketio.emit('last_quote_array_response_event',
                              {"error": "no data",
                               "message": "try to set different time period"}, to=request.sid)
            else:
                cursor = last_quote_array.find(query).sort("LastTradeTime", DESCENDING)
                for doc in cursor:
                    doc["_id"] = str(doc["_id"])
                    json_data = json.dumps(doc)
                    socketio.emit('last_quote_array_response_event', json_data, to=request.sid)
        else:
            # Latest data query
            query = data['query']
            latest = last_quote_array.find_one(query, sort=[("LastTradeTime", DESCENDING)])
            latest["_id"] = str(latest["_id"])
            json_data = json.dumps(latest)
            socketio.emit('last_quote_array_response_event', json_data, to=request.sid)

    except Exception as e:
        json_data = {"Error": str(e)}
        socketio.emit('last_quote_array_response_event', json_data, to=request.sid)


@socketio.on('last_quote_option_greek')
def handle_last_quote_option_greek(data):
    """
       Handle the 'last_quote_option_greek' WebSocket event by querying and emitting option Greek data.

       Args:
           data (dict): A dictionary containing query parameters, including 'period' for historical data and 'query' for
                        specifying query conditions.

       Emits:
           - 'last_quote_array_response_event': Emits JSON data containing the query result.

       """
    try:
        last_quote_option_greek = db.last_quote_option_greek

        if data.get('period'):
            # Historical data query
            start_timestamp = datetime.strptime(data['period']['start'], "%Y-%m-%d").timestamp()
            end_timestamp = datetime.strptime(data['period']['end'], "%Y-%m-%d").timestamp()
            query = {
                "Token": data['query']['Token'],
                "LastTradeTime": {"$gte": int(start_timestamp), "$lte": int(end_timestamp)}
            }
            cursor = last_quote_option_greek.find(query).sort("LastTradeTime", DESCENDING)
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                json_data = json.dumps(doc)
                socketio.emit('last_quote_option_greek_response_event', json_data, to=request.sid)
        else:
            # Latest data query
            query = data['query']
            latest = last_quote_option_greek.find_one(query, sort=[("LastTradeTime", DESCENDING)])
            latest["_id"] = str(latest["_id"])
            json_data = json.dumps(latest)
            socketio.emit('last_quote_option_greek_response_event', json_data, to=request.sid)

    except Exception as e:
        json_data = {"Error": str(e)}
        socketio.emit('last_quote_option_greek_response_event', json_data, to=request.sid)


@socketio.on('last_quote_option_greek_chain')
def handle_last_quote_option_greek_chain(data):
    """
        Handle the 'last_quote_option_greek_chain' WebSocket event by querying and emitting option Greek chain data.

        Args:
            data (dict): A dictionary containing query parameters, including 'period' for historical data and 'query' for
                         specifying query conditions.

        Emits:
            - 'last_quote_array_response_event': Emits JSON data containing the query result.

        """
    try:
        last_quote_option_greek_chain = db.last_quote_option_greek_chain

        if data.get('period'):
            # Historical data query
            start_timestamp = datetime.strptime(data['period']['start'], "%Y-%m-%d").timestamp()
            end_timestamp = datetime.strptime(data['period']['end'], "%Y-%m-%d").timestamp()
            query = {
                "InstrumentIdentifier": data['query']['InstrumentIdentifier'],
                "LastTradeTime": {"$gte": int(start_timestamp), "$lte": int(end_timestamp)}
            }
            cursor = last_quote_option_greek_chain.find(query).sort("LastTradeTime", DESCENDING)
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                json_data = json.dumps(doc)
                socketio.emit('last_quote_option_greek_chain_response_event', json_data, to=request.sid)
        else:
            # Latest data query
            query = data['query']
            latest = last_quote_option_greek_chain.find_one(query, sort=[("LastTradeTime", DESCENDING)])
            latest["_id"] = str(latest["_id"])
            json_data = json.dumps(latest)
            socketio.emit('last_quote_option_greek_chain_response_event', json_data, to=request.sid)

    except Exception as e:
        json_data = {"Error": str(e)}
        socketio.emit('last_quote_option_greek_chain_response_event', json_data, to=request.sid)


if __name__ == '__main__':
    socketio.run(app, allow_unsafe_werkzeug=True)
