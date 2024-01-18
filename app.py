from app import app
from app.controllers.quote_controller import QuoteController
from flask import request

# Routes
@app.route('/get_ltp', methods=['GET'])
def get_ltp():
    instrument_identifier = request.args.get('instrument_identifier')
    return QuoteController.get_ltp(instrument_identifier)

@app.route('/get_all_instrument_identifiers', methods=['GET'])
def get_all_instrument_identifiers():
    return QuoteController.get_all_instrument_identifiers()

@app.route('/store_order', methods=['POST'])
def store_order():
    return QuoteController.store_order()

if __name__ == '__main__':
    app.run(debug=True)
