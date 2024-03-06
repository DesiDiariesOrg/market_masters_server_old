from flask import jsonify, request
from app.models.quote_model import QuoteModel

class QuoteController:
    @staticmethod
    def get_ltp(instrument_identifier):
        last_trade_price = QuoteModel.get_last_trade_price(instrument_identifier)
        return jsonify({'InstrumentIdentifier': instrument_identifier, 'LastTradePrice': last_trade_price}) if last_trade_price else jsonify({'error': 'Instrument not found'}), 404

    @staticmethod
    def get_all_instrument_identifiers():
        identifiers = QuoteModel.get_all_instrument_identifiers()
        return jsonify({'instrument_identifiers': identifiers})

    @staticmethod
    def buy_order():
        data = request.get_json()
        instrument_identifier = data.get('instrument_identifier')
        quantity = data.get('quantity')

        if not instrument_identifier or not quantity:
            return jsonify({'error': 'Instrument Identifier and Quantity are required'}), 400

        QuoteModel.buy_order(instrument_identifier, quantity)
        return jsonify({'success': 'Order stored successfully'}), 201
