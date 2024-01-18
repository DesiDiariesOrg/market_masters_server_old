import asyncio
import datetime
import json
import os

import pytz
import websockets
from pymongo import MongoClient, errors
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv('DB_URL')
accesskey = os.getenv('API_KEY')
endpoint = os.getenv('API_endpoint')

client = MongoClient(uri)
db = client.last_quote


def is_weekday_and_market_hours(dt: datetime) -> bool:
    # Check if it's a weekday (Monday to Friday) and the time is within market hours starting from 9:10 AM
    return dt.weekday() <= 5 and 9 <= dt.hour < 17 and (dt.hour != 17 or dt.minute < 30) and (
            dt.hour != 9 or dt.minute >= 10)


def add_custom_fields(content):
    for item in content:
        stock_name = item['InstrumentIdentifier'].split('_')[1]
        date = item['InstrumentIdentifier'].split('_')[2]
        datetime_object = datetime.datetime.strptime(date, "%d%b%Y")
        utc_offset = datetime.timezone(datetime.timedelta(hours=0))  # UTC timezone
        formatted_date = datetime_object.replace(tzinfo=utc_offset).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        stock_price = item['InstrumentIdentifier'].split('_')[-1]
        item['stock_name'] = stock_name
        item['date'] = formatted_date
        item['stock_price'] = stock_price
    return content


async def create_collections():
    try:
        # creating capped collection max size 100mb for last_quote_array, 400mb for last_quote_option_greek_chain
        db.create_collection("last_quote_array", capped=True, size=100000000, check_exists=True)
        # db.create_collection("last_quote_option_greek", capped=True, size=100000000, check_exists=True)
        db.create_collection("last_quote_option_greek_chain", capped=True, size=400000000, check_exists=True)
    except errors.CollectionInvalid:
        print('Creating collection skipped, as it is already exists')


async def authenticate(websocket):
    print("Work in progress...")
    # print("Connecting to endpoint : " + endpoint)
    str_message = '{"MessageType":"Authenticate","Password":"' + accesskey + '"}'
    await websocket.send(str_message)
    # print("Sent Authentication Message : " + str_message)
    response = await websocket.recv()
    all_res = response.split(',')
    str_complete = all_res[0].split(':')
    result = str_complete[1]
    while result != "true":
        response = await websocket.recv()
        all_res = response.split(',')
        str_complete = all_res[0].split(':')
        result = str_complete[1]
        print(response)
    if result == "true":
        print("Authentication is Complete!!!")


async def get_last_quote_option_greek_chain(websocket, product: str):
    while True:
        exchange_name = "NFO"
        str_message = {"MessageType": "GetLastQuoteOptionGreeksChain", "Exchange": exchange_name, "Product": product}
        query = json.dumps(str_message)
        await websocket.send(query)
        # print(f"Message sent : {str_message}")
        # print("Waiting for response...")
        await asyncio.sleep(1)


async def get_last_quote_array(websocket, instrument_identifier: list):
    while True:
        chunk_size = 25
        for i in range(0, len(instrument_identifier), chunk_size):
            chunk = instrument_identifier[i:i + chunk_size]
            if not chunk:
                continue
            exchange_name = "NFO"
            str_message = {"MessageType": "GetLastQuoteArray", "Exchange": exchange_name,
                           "InstrumentIdentifiers": chunk}
            query = json.dumps(str_message)
            await websocket.send(query)
            # print(f"Message sent : {str_message}")
            # print("Waiting for response...")
        await asyncio.sleep(30)


async def handle_responses(websocket):
    while True:
        try:
            message = await websocket.recv()
            result = json.loads(message)
            if result["MessageType"] == 'OptionGreeksChainWithQuoteResult':
                data_dict = json.loads(message)
                if data_dict['Result']:
                    print(data_dict['MessageType'])
                    data_with_added_fields = add_custom_fields(data_dict['Result'])
                    # Add documents to MongoDB
                    # last_quote_option_greek_chain = db.last_quote_option_greek_chain  # Set collection
                    # last_quote_option_greek_chain.insert_many(data_with_added_fields)
            elif result["MessageType"] == 'LastQuoteArrayResult':
                data_dict = json.loads(message)
                print(data_dict['MessageType'])
                # # Add documents to MongoDB
                # last_quote_array = db.last_quote_array  # Set collection
                # last_quote_array.insert_many(data_dict['Result'])
        except websockets.ConnectionClosedOK:
            break


async def main():
    # indian_timezone = pytz.timezone('Asia/Kolkata')
    # time_now = datetime.datetime.now(indian_timezone)
    # if is_weekday_and_market_hours(time_now):
    await create_collections()
    async with websockets.connect(endpoint) as websocket:
        await authenticate(websocket)
        last_quote_array_task = asyncio.create_task(get_last_quote_array(websocket, identifiers))

        tasks = [asyncio.create_task(get_last_quote_option_greek_chain(websocket, company)) for company in
                 companies]
        response_waiter = asyncio.create_task(handle_responses(websocket))
        await asyncio.gather(*tasks, last_quote_array_task, response_waiter)
    # else:
    #     print("Exchange is closed")


if __name__ == "__main__":
    companies = ['ADANI ENTERPRISES', 'ADANI PORTS', 'APOLLO HOSPITAL', 'ASIAN PAINTS', 'AXIS BANK', 'BAJAJ AUTO',
                 'BAJAJ FINANCE', 'BAJAJ FINSERV', 'BHARTI AIRTEL', 'BPCL', 'BRITANNIA', 'CIPLA',
                 'COAL INDIA', 'DIVIS LABS', 'DR REDDYS LABS', 'EICHER MOTORS', 'GRASIM', 'HCL TECH', 'HDFC',
                 'HDFC BANK', 'HDFC LIFE', 'HERO MOTOCORP', 'HINDALCO', 'HUL', 'ICICI BANK', 'INDUSIND BANK',
                 'INFOSYS', 'ITC', 'JSW STEEL', 'KOTAK MAHINDRA', 'LARSEN', 'M&M', 'MARUTI SUZUKI', 'NESTLE',
                 'NTPC', 'ONGC', 'POWER GRID CORP', 'RELIANCE', 'SBI', 'SBI LIFE INSURA', 'SUN PHARMA',
                 'TATA CONS. PROD', 'TATA MOTORS', 'TATA STEEL', 'TCS', 'TECH MAHINDRA',
                 'TITAN COMPANY', 'ULTRATECHCEMENT', 'UPL', 'WIPRO']

    identifiers = [{"Value": "NIFTY-I"}, {"Value": "BANKNIFTY-I"}, {"Value": "MIDCAPNIFTY-I"}, {"Value": "FINNIFTY-I"},
                   {"Value": "AARTIIND-I"}, {"Value": "ACC-I"}, {"Value": "ADANIENT-I"}, {"Value": "ADANIPORTS-I"},
                   {"Value": "AMBUJACEM-I"}, {"Value": "APOLLOHOSP-I"},
                   {"Value": "ASHOKLEY-I"}, {"Value": "ASIANPAINT-I"}, {"Value": "ASTRAL-I"}, {"Value": "AUBANK-I"},
                   {"Value": "AUROPHARMA-I"}, {"Value": "ASHOKLEY-I"}, {"Value": "BAJAJ-AUTO-I"},
                   {"Value": "BAJAJFINSV-I"},
                   {"Value": "BAJFINANCE-I"}, {"Value": "BANDHANBNK-I"}, {"Value": "BANKBARODA-I"},
                   {"Value": "BATAINDIA-I"}, {"Value": "BEL-I"}, {"Value": "BERGEPAINT-I"},
                   {"Value": "BHARATFORG-I"}, {"Value": "BHARTIARTL-I"}, {"Value": "BHEL-I"}, {"Value": "BIOCON-I"},
                   {"Value": "BOSCHLTD-I"}, {"Value": "BPCL-I"}, {"Value": "BRITANNIA-I"}, {"Value": "CANBK-I"},
                   {"Value": "CANFINHOME-I"}, {"Value": "CHOLAFIN-I"},
                   {"Value": "CIPLA-I"}, {"Value": "COALINDIA-I"}, {"Value": "COFORGE-I"}, {"Value": "COLPAL-I"},
                   {"Value": "CONCOR-I"},
                   {"Value": "COROMANDEL-I"}, {"Value": "CUMMINSIND-I"}, {"Value": "DABUR-I"}, {"Value": "DEEPAKNTR-I"},
                   {"Value": "DIVISLAB-I"},
                   {"Value": "DLF-I"}, {"Value": "DRREDDY-I"}, {"Value": "EICHERMOT-I"}, {"Value": "ESCORTS-I"},
                   {"Value": "EXIDEIND-I"},
                   {"Value": "FEDERALBNK-I"}, {"Value": "GAIL-I"}, {"Value": "GODREJCP-I"}, {"Value": "GODREJPROP-I"},
                   {"Value": "GRASIM-I"},
                   {"Value": "GUJGASLTD-I"}, {"Value": "HAL-I"}, {"Value": "HAVELLS-I"}, {"Value": "HCLTECH-I"},
                   {"Value": "HDFC-I"},
                   {"Value": "HDFCAMC-I"}, {"Value": "HDFCBANK-I"}, {"Value": "HDFCLIFE-I"}, {"Value": "HEROMOTOCO-I"},
                   {"Value": "HINDALCO-I"},
                   {"Value": "HINDPETRO-I"}, {"Value": "HINDUNILVR-I"}, {"Value": "ICICIBANK-I"},
                   {"Value": "ICICIGI-I"},
                   {"Value": "ICICIPRULI-I"}, {"Value": "IDEA-I"}, {"Value": "IDFCFIRSTB-I"}, {"Value": "IEX-I"},
                   {"Value": "IGL-I"}, {"Value": "INDHOTEL-I"},
                   {"Value": "INDIAMART-I"}, {"Value": "INDIGO-I"}, {"Value": "INDUSINDBK-I"},
                   {"Value": "INDUSTOWER-I"}, {"Value": "INFY-I"},
                   {"Value": "IOC-I"}, {"Value": "IPCALAB-I"}, {"Value": "IRCTC-I"}, {"Value": "ITC-I"},
                   {"Value": "JINDALSTEL-I"},
                   {"Value": "JSWSTEEL-I"}, {"Value": "JUBLFOOD-I"}, {"Value": "KOTAKBANK-I"},
                   {"Value": "LALPATHLAB-I"},
                   {"Value": "LICHSGFIN-I"}, {"Value": "LT-I"}, {"Value": "LTTS-I"}, {"Value": "LUPIN-I"},
                   {"Value": "M&M-I"}, {"Value": "M&MFIN-I"},
                   {"Value": "MANAPPURAM-I"}, {"Value": "MARICO-I"}, {"Value": "MARUTI-I"}, {"Value": "MCDOWELL-N-I"},
                   {"Value": "METROPOLIS-I"},
                   {"Value": "MFSL-I"}, {"Value": "MGL-I"}, {"Value": "MPHASIS-I"}, {"Value": "MUTHOOTFIN-I"},
                   {"Value": "NAUKRI-I"},
                   {"Value": "NAVINFLUOR-I"}, {"Value": "NESTLEIND-I"}, {"Value": "NMDC-I"}, {"Value": "NTPC-I"},
                   {"Value": "OFSS-I"},
                   {"Value": "ONGC-I"}, {"Value": "PEL-I"}, {"Value": "PFC-I"}, {"Value": "PIDILITIND-I"},
                   {"Value": "PNB-I"},
                   {"Value": "POLYCAB-I"}, {"Value": "POWERGRID-I"}, {"Value": "RBLBANK-I"}, {"Value": "RECLTD-I"},
                   {"Value": "RELIANCE-I"},
                   {"Value": "SAIL-I"}, {"Value": "SBILIFE-I"}, {"Value": "SBIN-I"}, {"Value": "SRF-I"},
                   {"Value": "SUNPHARMA-I"},
                   {"Value": "SYNGENE-I"}, {"Value": "TATACHEM-I"}, {"Value": "TATACONSUM-I"},
                   {"Value": "TATAMOTORS-I"},
                   {"Value": "TATAPOWER-I"}, {"Value": "TATASTEEL-I"}, {"Value": "TCS-I"}, {"Value": "TECHM-I"},
                   {"Value": "TITAN-I"}, {"Value": "TRENT-I"},
                   {"Value": "TVSMOTOR-I"}, {"Value": "UBL-I"}, {"Value": "ULTRACEMCO-I"}, {"Value": "UPL-I"},
                   {"Value": "VEDL-I"},
                   {"Value": "VOLTAS-I"}, {"Value": "WIPRO-I"}, {"Value": "ZEEL-I"}]

    asyncio.run(main())
