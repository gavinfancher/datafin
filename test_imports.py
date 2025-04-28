from datafin import APICall, get_trading_days

# Test APICall import
api = APICall(base_url="https://api.example.com", api_key="test")
print("APICall imported successfully:", api)

# Test datetime utils import
trading_days = get_trading_days()
print("Trading days imported successfully:", trading_days[:5] if trading_days else "No trading days") 