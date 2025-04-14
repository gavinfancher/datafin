from DataFin import FMPClient
import os
import sys
import dotenv
import time

dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('S3_BUCKET_NAME')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

fmp = FMPClient(fmp_api_key)

symbol = sys.argv[1]
print(symbol)

try:
    print("\nPress Ctrl+C to stop the program")
    print(f"{'Price':<10} {'Change':<10}")
    print("-" * 32)
    
    while True:
        amzn_day_start = fmp.get_end_of_day_full('amzn')[0]['open']
        
        data = fmp.get_live_price(symbol)
        price = data[0]['price']
        pct_change = round((data[0]['change'] / amzn_day_start) * 100, 2)
        

        print(f"{price:<10} {pct_change:>6}%")
        

except KeyboardInterrupt:
    print("\nProgram stopped by user")