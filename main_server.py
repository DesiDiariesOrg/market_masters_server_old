from flask import Flask, request, jsonify
from pymongo import MongoClient
import certifi

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient('mongodb+srv://manthangehlot66:05Upd9TTlReeMNlR@optionaro.f2tgbib.mongodb.net/?retryWrites=true&w=majority', tlsCAFile=certifi.where())
db = client['OptionARO']
last_quote_array = db['last_quote_array']

@app.route('/get_ltp', methods=['GET'])
def get_ltp():
    # Get instrument identifier from the query parameters
    instrument_identifier = request.args.get('instrument_identifier')

    # Retrieve the document from MongoDB based on the instrument identifier
    document = last_quote_array.find_one({'InstrumentIdentifier': instrument_identifier})

    if document:
        # Extract last trade price
        last_trade_price = document['LastTradePrice']
        return jsonify({'InstrumentIdentifier': instrument_identifier, 'LastTradePrice': last_trade_price})
    else:
        return jsonify({'error': 'Instrument not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)
