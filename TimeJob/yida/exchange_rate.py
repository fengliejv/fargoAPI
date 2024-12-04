import json

import requests

from service.BankExchangeRateService import add_bank_exchange_rate


def sync_exchange_rate():
    try:
        payload = {}
        headers = {
            "apikey": "yJmlySeDi17SEAWjp0MH4fD9eesmV8kP"
        }
        currency = ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'HKD', 'JPY', 'SGD', 'USD']
        symbols = "AED"
        for i in currency:
            symbols = symbols + "%2C" + i
        url = f"https://api.apilayer.com/exchangerates_data/latest?symbols={symbols}&base=USD"
        response = requests.request("GET", url, headers=headers, data=payload)
        status_code = response.status_code

        if status_code != 200:
            return
        result = json.loads(response.text)
        print(result['rates'])
        for i in result['rates']:
            add_bank_exchange_rate(currency=i, rate=float(1 / result['rates'][i]), date=result['date'],
                                   timestamp=result['timestamp'])
    except Exception as e:
        print(str(e))


if __name__ == '__main__':
    sync_exchange_rate()
