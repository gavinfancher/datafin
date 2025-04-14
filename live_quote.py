from DataFin import FMPClient
import os
import dotenv
import time

dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('S3_BUCKET_NAME')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

fmp = FMPClient(fmp_api_key)

try:
    print("Press Ctrl+C to stop the program")
    print(f"{'Price':<11} {'Change':<11}")
    print("-" * 10)
    
    while True:
        amzn_day_start = fmp.get_end_of_day_full('amzn')[0]['open']
        
        amzn = fmp.get_live_price('amzn')
        price = amzn[0]['price']
        pct_change = round((amzn[0]['change'] / amzn_day_start) * 100, 2)
        

        print(f"{price:<11}' {pct_change:>6}%")
        
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\nProgram stopped by user")