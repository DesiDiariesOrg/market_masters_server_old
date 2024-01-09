from pymongo import MongoClient
import certifi

# Connect to MongoDB
client = MongoClient('mongodb+srv://manthangehlot66:05Upd9TTlReeMNlR@optionaro.f2tgbib.mongodb.net/?retryWrites=true&w=majority', tlsCAFile=certifi.where())
db = client['OptionARO']
last_quote_array = db['last_quote_array']

# Retrieve data from MongoDB
cursor = last_quote_array.find()

# Print 'Instrument Identifier' and 'Last Trade Price' for each document
for document in cursor:
    instrument_identifier = document['InstrumentIdentifier']
    last_trade_price = document['LastTradePrice']
    print(f"Instrument Identifier: {instrument_identifier}, Last Trade Price: {last_trade_price}")

# Close MongoDB connection
client.close()
