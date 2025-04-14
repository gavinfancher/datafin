from DataFin import FMPClient
import os
import sys
import dotenv
import time
from flask import Flask, render_template
from flask_socketio import SocketIO
import threading

# Load environment variables
dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')
s3_bucket = os.getenv('S3_BUCKET_NAME')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize FMP client
fmp = FMPClient(fmp_api_key)

# Get symbol from command line argument or use default
symbol = sys.argv[1] if len(sys.argv) > 1 else 'AAPL'
print(f"Tracking: {symbol}")

# Initialize Flask and SocketIO
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Create a flag to control the background thread
thread_stop_event = threading.Event()

# Function to get stock price and emit to client
def get_stock_data():
    while not thread_stop_event.is_set():
        try:
            # Get day opening price for percentage calculation
            symbol_day_start = fmp.get_end_of_day_full(symbol)[0]['open']
            
            # Get live price data
            data = fmp.get_live_price(symbol)
            price = data[0]['price']
            pct_change = round((data[0]['change'] / symbol_day_start) * 100, 2)
            
            # Emit data to clients
            socketio.emit('stock_update', {'price': price, 'pct_change': pct_change})
            
            # Wait before next update
        except Exception as e:
            print(f"Error: {e}")

# Route for the home page
@app.route('/')
def index():
    return render_template('index.html', symbol=symbol)

# Socket.IO event handling
@socketio.on('connect')
def handle_connect():
    print('Client connected')
    
@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

# Create and start the background thread
def start_background_thread():
    global thread
    if not thread_stop_event.is_set():
        thread = threading.Thread(target=get_stock_data)
        thread.daemon = True
        thread.start()

# Create templates directory and HTML file
def setup_templates():
    import os
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    with open('templates/index.html', 'w') as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>Stock Tracker - {{ symbol }}</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            text-align: center;
        }
        h1 {
            color: #333;
        }
        .stock-data {
            margin: 30px 0;
            padding: 20px;
            background-color: #f5f5f5;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .price {
            font-size: 3em;
            font-weight: bold;
            margin: 10px 0;
        }
        .change {
            font-size: 1.5em;
            padding: 5px 10px;
            border-radius: 4px;
            display: inline-block;
        }
        .positive {
            background-color: #d4edda;
            color: #155724;
        }
        .negative {
            background-color: #f8d7da;
            color: #721c24;
        }
        .last-update {
            font-size: 0.8em;
            color: #6c757d;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <h1>{{ symbol }} Stock Tracker</h1>
    <div class="stock-data">
        <div id="price" class="price">Loading...</div>
        <div id="pct-change" class="change">-</div>
        <div class="last-update">Last update: <span id="last-update">-</span></div>
    </div>

    <script>
        // Connect to Socket.IO server
        const socket = io();
        
        // Listen for stock updates
        socket.on('stock_update', function(data) {
            // Update price
            document.getElementById('price').textContent = '$' + data.price.toFixed(2);
            
            // Update percentage change
            const pctChangeEl = document.getElementById('pct-change');
            const changeValue = data.pct_change;
            pctChangeEl.textContent = (changeValue > 0 ? '+' : '') + changeValue.toFixed(2) + '%';
            pctChangeEl.className = 'change ' + (changeValue >= 0 ? 'positive' : 'negative');
            
            // Update timestamp
            const now = new Date();
            document.getElementById('last-update').textContent = now.toLocaleTimeString();
        });
    </script>
</body>
</html>
        """)

if __name__ == '__main__':
    # Create HTML template
    setup_templates()
    
    # Start background thread
    thread = None
    start_background_thread()
    
    # Start Flask server
    print("Starting web server. Access at http://127.0.0.1:1000/")
    socketio.run(app, debug=True, host='0.0.0.0', port=1000)