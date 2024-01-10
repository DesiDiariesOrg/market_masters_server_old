from flask import Flask, request, jsonify
from pymongo import MongoClient
import certifi

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient('mongodb+srv://manthangehlot66:05Upd9TTlReeMNlR@optionaro.f2tgbib.mongodb.net/?retryWrites=true&w=majority', tlsCAFile=certifi.where())
db = client['OptionARO']
last_quote_array = db['last_quote_array']

# Models
class QuoteModel:
    @staticmethod
    def get_last_trade_price(instrument_identifier):
        document = last_quote_array.find_one({'InstrumentIdentifier': instrument_identifier})
        return document['LastTradePrice'] if document else None

    @staticmethod
    def get_all_instrument_identifiers():
        cursor = last_quote_array.find({}, {'InstrumentIdentifier': 1, '_id': 0})
        return [doc['InstrumentIdentifier'] for doc in cursor]

# Controllers
class QuoteController:
    @staticmethod
    def get_ltp(instrument_identifier):
        last_trade_price = QuoteModel.get_last_trade_price(instrument_identifier)
        return jsonify({'InstrumentIdentifier': instrument_identifier, 'LastTradePrice': last_trade_price}) if last_trade_price else jsonify({'error': 'Instrument not found'}), 404

    @staticmethod
    def get_all_instrument_identifiers():
        identifiers = QuoteModel.get_all_instrument_identifiers()
        return jsonify({'instrument_identifiers': identifiers})

# Routes
@app.route('/get_ltp', methods=['GET'])
def get_ltp():
    instrument_identifier = request.args.get('instrument_identifier')
    return QuoteController.get_ltp(instrument_identifier)

@app.route('/get_all_instrument_identifiers', methods=['GET'])
def get_all_instrument_identifiers():
    return QuoteController.get_all_instrument_identifiers()

if __name__ == '__main__':
    app.run(debug=True)
