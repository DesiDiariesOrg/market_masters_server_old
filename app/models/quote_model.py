from datetime import datetime
from pymongo import MongoClient
import certifi

# Connect to MongoDB
client = MongoClient('mongodb+srv://manthangehlot66:05Upd9TTlReeMNlR@optionaro.f2tgbib.mongodb.net/?retryWrites=true&w=majority', tlsCAFile=certifi.where())
db = client['OptionARO']
last_quote_array = db['last_quote_array']
order_collection = db['orders']

class QuoteModel:
    @staticmethod
    def get_last_trade_price(instrument_identifier):
        document = last_quote_array.find_one({'InstrumentIdentifier': instrument_identifier})
        return document['LastTradePrice'] if document else None

    @staticmethod
    def get_all_instrument_identifiers():
        cursor = last_quote_array.find({}, {'InstrumentIdentifier': 1, '_id': 0})
        return [doc['InstrumentIdentifier'] for doc in cursor]

    @staticmethod
    def store_order(instrument_identifier, quantity):
        # Get LTP at the time of the order
        ltp = QuoteModel.get_last_trade_price(instrument_identifier)

        order = {
            'InstrumentIdentifier': instrument_identifier,
            'Quantity': quantity,
            'LTP': ltp,
            'OrderTime': datetime.now()
        }
        order_collection.insert_one(order)
