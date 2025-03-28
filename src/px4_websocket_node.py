import time
import asyncio
import websockets
import json
import rclpy
from rclpy.node import Node
import uuid
import hmac
import jwt
import base64
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from rclpy.qos import QoSProfile, ReliabilityPolicy
import numpy as np
import math
import importlib
from collections import deque
import requests
import socket

# ===== Configuration =====
WEBSOCKET_SERVER = "ws://localhost:8082"
HTTP_URL = 'http://localhost:5000/api'

DRONE_ID = str(uuid.uuid4())

# Security configuration
SECRET_KEY = 'dsksupplytech' 
SECRET_MESSAGE_KEY = b'kchar-long-secret-key-1234567890'
# =========================

def get_local_ip():
    """Получает локальный IP-адрес устройства"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "unknown"

def topic_to_camel(topic: str) -> str:
    return ''.join(word.capitalize() for word in topic.split('/')[-1].split('_'))

def is_token_expired(token: str) -> bool:
    """Проверяет, истек ли срок действия токена."""
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        expiration_time = decoded.get('exp')
        if expiration_time is None:
            return True
        return time.time() >= expiration_time
    except Exception as e:
        print(f"Error decoding token: {e}")
        return True

# Подпись сообщения
def sign_message(message: str) -> str:
    """Generate HMAC signature for message"""
    return hmac.new(SECRET_KEY.encode(), message.encode(), sha256).hexdigest()

def encrypt_data(data) -> str:
    """Encrypt data (both strings and bytes) with AES-CBC"""
    # Преобразуем данные в JSON-строку, если это dict или list
    if not isinstance(data, bytes):
        data = json.dumps(data)
    if isinstance(data, str):
        data = data.encode('utf-8')
    cipher = AES.new(SECRET_MESSAGE_KEY, AES.MODE_CBC)
    ct_bytes = cipher.encrypt(pad(data, AES.block_size))
    iv = base64.b64encode(cipher.iv).decode('utf-8')
    ct = base64.b64encode(ct_bytes).decode('utf-8')
    return json.dumps({
        'iv': iv,
        'ciphertext': ct,
        'signature': sign_message(ct)
    })

# def decrypt_data(encrypted_data: str) -> str:
#     """Decrypt AES-CBC encrypted data"""
#     data = json.loads(encrypted_data)
#     iv = base64.b64decode(data['iv'])
#     ct = base64.b64decode(data['ciphertext'])
#     cipher = AES.new(SECRET_MESSAGE_KEY, AES.MODE_CBC, iv)
#     pt = unpad(cipher.decrypt(ct), AES.block_size)
#     return pt.decode('utf-8')

class PX4WebSocketBridge(Node):
    def __init__(self):
        super().__init__('px4_websocket_bridge')
        
        self.message_queue = deque()
        self.ack_queue = asyncio.Queue()

        self.qos_profile = QoSProfile(depth=10)
        self.qos_profile.reliability = ReliabilityPolicy.BEST_EFFORT

        self.access_token = None
        self.refresh_token = None

        self.local_ip = get_local_ip()
        
        self.authenticate()

    def ensure_tokens_are_valid(self) -> bool:
            """Проверяет актуальность токенов и обновляет их при необходимости."""
            if not self.access_token or is_token_expired(self.access_token):
                print("Access token is expired or missing. Refreshing...")
                if not self.refresh_token or is_token_expired(self.refresh_token):
                    print("Refresh token is expired or missing. Authenticating...")
                    return self.authenticate()  
                else:
                    return self.refresh_access_token()  
            return True 


    def refresh_access_token(self):
        """Обновление Access Token с использованием Refresh Token."""
        if not self.refresh_token or is_token_expired(self.refresh_token):
            return False
        try:
            response = requests.post(
                f"{HTTP_URL}/refresh",
                json={'encrypted_data':encrypt_data(self.refresh_token)},
            )
            if response.status_code == 200:
                self.access_token = response.json().get('accessToken')
                print("Access token refreshed successfully.")
                return True
            else:
                print(f"Failed to refresh access token: {response}")
                return False
        except Exception as e:
            print(f"Error refreshing access token: {e}")
            return False


    def authenticate(self,retry_delay=1):
        """Авторизация дрона на сервере с повторными попытками.""" 
        time.sleep(3) # Необходимо дождаться полной инициализации PX4, иначе не все топики будут загружены
        payload = {
            'drone_id': DRONE_ID, 
            'topics': self.get_all_active_topics(),
            'ip_address': self.local_ip
        }
        while True:
            try:
                response = requests.post(
                    f"{HTTP_URL}/auth", 
                    json={'encrypted_data':encrypt_data(payload)},
                )
                if response.status_code == 200:
                    self.access_token = response.json().get('accessToken')
                    self.refresh_token = response.json().get('refreshToken')

                    if not self.access_token or not self.refresh_token:
                        print("Invalid tokens received from server.")
                        return False
                    
                    print("Authentication successful.")
                    return True   
                else:
                    print(f"Authentication failed: {response.status_code}")
                    return False
            except requests.exceptions.RequestException as e:
                print(f"Error during authentication: {e}")

            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)                 
            retry_delay = min(retry_delay * 2, 60)


    async def send_initial_data(self):
        """Отправка начальных данных при подключении к WebSocket"""
        # Отправляем список топиков
        info_message = {
            'type': 'info',
            'topics': self.get_all_active_topics(),
            'ip': self.local_ip
        }
        await self.send_data_with_ack(info_message)


    async def send_data_with_ack(self, content, max_retries=3):
        """Отправка данных с механизмом гарантированной доставки АСК."""
        for attempt in range(max_retries):
            try:
                if not self.ensure_tokens_are_valid():
                    print("Failed to ensure tokens are valid.")
                    return
                
                encrypted_data = encrypt_data(content)

                await self.websocket.send(encrypted_data)

                ack_recieved = await asyncio.wait_for(self.ack_queue.get(), timeout=5)
                if ack_recieved.get('type') == 'ack': 
                    return True
                else:
                    print(f"Attempt {attempt + 1}: No ACK received. Retrying...")
            except Exception as e:
                print(f"Error sending data: {e}")
            await asyncio.sleep(2)  
        print("Failed to deliver data after multiple attempts.")
        return False


    async def process_queue(self):
        """Обработка очереди сообщений."""
        while True:   
            try: 
                if self.message_queue:
                    content = self.message_queue.popleft()
                    await self.send_data_with_ack(content)
            except Exception as e:
                self.get_logger().error(f"Error processing data queue: {e}")
            await asyncio.sleep(0.01) 


    async def handle_websocket(self,retry_delay = 1):
        """Обработка WebSocket с экспоненциальной задержкой."""
        while True:
            try:
                if not self.ensure_tokens_are_valid():
                    print("Failed to ensure tokens are valid. Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    continue

                async with websockets.connect(
                    WEBSOCKET_SERVER,
                    additional_headers={
                        'Authorization': f'Bearer {self.access_token}'
                    }
                ) as websocket:
                    self.websocket = websocket
                    print("WebSocket connection established.")

                    await self.send_initial_data()
                    
                    process_task = asyncio.create_task(self.process_queue())

                    async for message in websocket:
                        data = json.loads(message)
                        if data.get('type') == 'ack':
                            await self.ack_queue.put(data) 
                        elif data.get('type') == 'subscribe':
                            self.subscribe(data.get('topic'))
                        elif data.get('type') == 'unsubscribe':
                            self.unsubscribe(data.get('topic'))
                        elif data.get('type') == 'refresh_token':
                            self.refresh_access_token()

                    process_task.cancel()
                    try:
                        await process_task
                    except asyncio.CancelledError:
                        print("Tasks cancelled.")
            except Exception as e:
                print(f"WebSocket error: {e}. Reconnecting in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
    

    def subscribe(self, topic_name):
        """Подписка на топик."""
        if topic_name in [sub.topic_name for sub in self.subscriptions]:
            print(f"Already subscribed to {topic_name}")
            return

        package = importlib.import_module('px4_msgs.msg')
      
        try:
            name = topic_to_camel(topic_name)
            msg_type = getattr(package, name)
        
            self.create_subscription(
                msg_type,
                topic_name,
                lambda msg, tn=topic_name: self.generic_callback(msg, tn), 
                self.qos_profile
            )
            print(f"Subscribed as {topic_name}")

        except AttributeError as e:
            print(f"Failed to find message type {topic_name}: {e}")
            return  


    def unsubscribe(self, topic_name):
        """Отписка от топика."""
        if not topic_name:
            print(f"Topic name not found for {topic_name}")
            return

        for sub in self.subscriptions:
            if sub.topic_name == topic_name:
                self.destroy_subscription(sub)
                print(f"Unsubscribed as {topic_name}")
                return
        print(f"No subscription found for {topic_name}")
   

    def generic_callback(self, msg, topic_name):
        """Коллбэк-функция вызывается при подписке на топик и добавляет данные топика в очередь на отправку"""
        try:
            result = self.extract_data(msg)
            data = {
                'type': 'data',
                'name': msg.__class__.__name__,
                'topic': topic_name,
                'timestamp': msg.timestamp if hasattr(msg, 'timestamp') else 0,
                'schema': result['schema'],
                'data': result['data']
            }
            self.message_queue.append(data)
        except Exception as e:
            self.get_logger().error(f"Error processing {topic_name}: {e}")


    def get_all_active_topics(self):
        """Получение списка активных топиков с их статусом подписки."""
        subscribed_topics = {sub.topic_name for sub in self.subscriptions}
        
        return [
            {
                'name': topic_to_camel(topic),
                'topic': topic,
                'status': topic in subscribed_topics
            }
            for topic, _ in self.get_topic_names_and_types()
        ]
  

    def extract_data(self, msg):
        """Универсальный метод для извлечения данных и схемы из сообщения PX4."""
        result = {
            'data': {},
            'schema': {"type": "object", "properties": {}}
        }
        
        if not hasattr(msg, '__slots__'):
            return result
        
        internal_fields = {'_header', '_stamp'}

        for field in msg.__slots__:
            if field in internal_fields:
                continue

            field_name = ''.join(word.capitalize() for word in field.lstrip('_').split('_'))
            value = getattr(msg, field)

            # Обработка данных
            if isinstance(value, (int, float, np.floating)):
                if math.isnan(value):
                    result['data'][field_name] = 'Nan'
                    result['schema']['properties'][field_name] = {"type": "string"}
                elif math.isinf(value):
                    result['data'][field_name] = "Infinity" if value > 0 else "-Infinity"
                    result['schema']['properties'][field_name] = {"type": "string"}
                else:
                    result['data'][field_name] = value
                    result['schema']['properties'][field_name] = {"type": "number"}
            
            elif isinstance(value, (list, tuple, np.ndarray)):
                if len(value) == 0:
                    result['data'][field_name] = 'None'
                    result['schema']['properties'][field_name] = {"type": "array", "items": {"type": "null"}}
                elif isinstance(value[0], (int, float, np.floating)):
                    result['data'][field_name] = [float(v) for v in value]
                    result['schema']['properties'][field_name] = {"type": "array", "items": {"type": "number"}}
                elif isinstance(value[0], str):
                    result['data'][field_name] = list(value)
                    result['schema']['properties'][field_name] = {"type": "array", "items": {"type": "string"}}
                else:
                    result['data'][field_name] = [self.extract_data(v)['data'] for v in value]
                    result['schema']['properties'][field_name] = {"type": "array", "items": self.extract_data(value[0])['schema']}
            
            elif hasattr(value, '__slots__'):
                nested_result = self.extract_data(value)
                result['data'][field_name] = nested_result['data']
                result['schema']['properties'][field_name] = nested_result['schema']
            
            else:
                result['data'][field_name] = str(value)
                result['schema']['properties'][field_name] = {"type": "string"}

        return result


async def spin_node(node):
    """Асинхронный spin для ROS 2 узла."""
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.1)
        await asyncio.sleep(0.1)

def main(args=None):
    rclpy.init(args=args)
    px4_websocket_bridge = PX4WebSocketBridge()

    loop = asyncio.get_event_loop()
    loop.create_task(spin_node(px4_websocket_bridge))
    loop.run_until_complete(px4_websocket_bridge.handle_websocket())
    
    loop.close()
    px4_websocket_bridge.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()


