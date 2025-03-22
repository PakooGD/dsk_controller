import rclpy
from rclpy.node import Node
import asyncio
import websockets
import json
from rclpy.qos import QoSProfile, ReliabilityPolicy
import numpy as np
import math
import importlib
from collections import defaultdict
import time
import requests
import uuid
import jwt
import hmac
from hashlib import sha256
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
from collections import deque

SECRET_KEY = 'dsksupplytech' 
DRONE_ID = str(uuid.uuid4())

SECRET_MESSAGE_KEY = b'kchar-long-secret-key-1234567890'

# Blocklist типов сообщений
UNSUPPORTED_MSG_TYPES = []

# Подпись сообщения
def sign_message(message: str) -> str:
    """Подписывает сообщение с использованием HMAC."""
    hash = hmac.new(SECRET_KEY.encode(),message.encode(),sha256).hexdigest()
    return hash

def camel_to_snake_case(name: str) -> str:
    return ''.join([' ' + char if char.isupper() else char for char in name]).strip().replace(' ', '_').lower()

def topic_to_camel(topic: str) -> str:
    if topic.startswith('/fmu/out/'):
        topic = topic[len('/fmu/out/'):]

    camelName = ''.join(word.capitalize() for word in topic.split('_'))
    
    return camelName

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

def encrypt_data(data: str) -> str:
    """Шифрует данные с использованием AES."""
    cipher = AES.new(SECRET_MESSAGE_KEY, AES.MODE_CBC)
    ct_bytes = cipher.encrypt(pad(data.encode(), AES.block_size))
    iv = base64.b64encode(cipher.iv).decode('utf-8')
    ct = base64.b64encode(ct_bytes).decode('utf-8')
    return json.dumps({'iv': iv, 'ciphertext': ct})

def decrypt_data(encrypted_data: str) -> str:
    """Дешифрует данные с использованием AES."""
    data = json.loads(encrypted_data)
    iv = base64.b64decode(data['iv'])
    ct = base64.b64decode(data['ciphertext'])
    cipher = AES.new(SECRET_MESSAGE_KEY, AES.MODE_CBC, iv)
    pt = unpad(cipher.decrypt(ct), AES.block_size)
    return pt.decode('utf-8')

class PX4WebSocketBridge(Node):
    def __init__(self):
        super().__init__('px4_websocket_bridge')
        
        self.message_queue = deque()
        self.ack_queue = asyncio.Queue()
        
        self.websocket_url = 'ws://localhost:8082'
        self.http_url = 'http://localhost:5000/api'

        self.qos_profile = QoSProfile(depth=10)
        self.qos_profile.reliability = ReliabilityPolicy.BEST_EFFORT

        self.access_token = None
        self.refresh_token = None

        self.authenticate()

    def ensure_tokens_are_valid(self) -> bool:
            """Проверяет актуальность токенов и обновляет их при необходимости."""
            if not self.access_token or is_token_expired(self.access_token):
                self.get_logger().info("Access token is expired or missing. Refreshing...")
                if not self.refresh_token or is_token_expired(self.refresh_token):
                    self.get_logger().info("Refresh token is expired or missing. Authenticating...")
                    return self.authenticate()  
                else:
                    return self.refresh_access_token()  
            return True 

    def refresh_access_token(self):
        """Обновление Access Token с использованием Refresh Token."""
        if not self.refresh_token or is_token_expired(self.refresh_token):
            self.get_logger().error("Refresh token is missing or expired.")
            return False

        try:
            response = requests.post(
                f"{self.http_url}/refresh",
                json={"refreshToken": self.refresh_token}
            )
            if response.status_code == 200:
                self.access_token = response.json().get('accessToken')
                self.get_logger().info("Access token refreshed successfully.")
                return True
            else:
                self.get_logger().error(f"Failed to refresh access token: {response}")
                return False
        except Exception as e:
            self.get_logger().error(f"Error refreshing access token: {e}")
            return False

    def authenticate(self):
        """Авторизация дрона на сервере с повторными попытками."""
        auth_data = {
            'drone_id': DRONE_ID,
        }
        retry_delay = 5

        while True:
            try:
                response = requests.post(f"{self.http_url}/auth", json=auth_data)
                
                if response.status_code == 200:
 
                    self.access_token = response.json().get('accessToken')
                    self.refresh_token = response.json().get('refreshToken')

                    if not self.access_token or not self.refresh_token:
                        self.get_logger().error("Invalid tokens received from server.")
                        return False

                    self.get_logger().info("Authentication successful.")
                    return True
                
                elif response.status_code == 401:  
                    self.get_logger().error("Authentication failed: Invalid drone_id.")
                    return False  
                
                else:
                    self.get_logger().error(f"Authentication failed: {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                self.get_logger().error(f"Error during authentication: {e}")
            
            # Ждем перед следующей попыткой
            self.get_logger().info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    def get_all_schemas(self):
        """Получение всех схем для топиков."""
        package = importlib.import_module('px4_msgs.msg')

        message_names = [msg for msg in dir(package) if msg[0].isupper()]  # Фильтр по заглавной букве
        
        schemas = []
        for name in message_names:
            if name in UNSUPPORTED_MSG_TYPES:
                continue
     
            topic_name = self.get_topic_name(name)
            
            if not self.is_topic_active(topic_name):
                continue

            msg = getattr(package, name)

            schema = self.generate_schema(msg)
            schemas.append({
                'topic': name,
                'encoding': "json",
                'schemaName': topic_name,   
                'schema': schema
            })
        return schemas

    def generate_schema(self, msg):
        """Генерация схемы для типа сообщения."""
        schema = {"type": "object", "properties": {}}

        if not hasattr(msg, '__slots__'):
            return schema
        
        internal_fields = {'_header', '_stamp'}

        for field in msg.__slots__:
            if field in internal_fields:
                continue

            field_name = ''.join(word.capitalize() for word in field.lstrip('_').split('_'))
            value = getattr(msg, field)

            if isinstance(value, (int, float, np.floating)):
                if math.isnan(value) or math.isinf(value):
                    schema["properties"][field_name] = {"type": "string"}
                else:
                    schema["properties"][field_name] = {"type": "number"}
            elif isinstance(value, (list, tuple, np.ndarray)):
                if len(value) == 0:
                    schema["properties"][field_name] = {"type": "array", "items": {"type": "null"}}  # или другой тип по умолчанию
                elif isinstance(value[0], (int, float, np.floating)):
                    schema["properties"][field_name] = {"type": "array", "items": {"type": "number"}}
                elif isinstance(value[0], str):
                    schema["properties"][field_name] = {"type": "array", "items": {"type": "string"}}
                else:
                    schema["properties"][field_name] = {"type": "array", "items": self.generate_schema(value[0])}
            elif hasattr(value, '__slots__'):  
                schema["properties"][field_name] = self.generate_schema(value)
            else:
                schema["properties"][field_name] = {"type": "string"} 

        return schema

    async def process_queue(self):
        """Обработка очереди сообщений."""
        while self.message_queue:
            type, content = self.message_queue[0]
            if await self.send_data_with_ack(type, content):
                self.message_queue.popleft()  # Удаляем сообщение после успешной доставки
            else:
                break  # Прерываем цикл при ошибке

    async def wait_for_ack(self):
        """Ожидание подтверждения (ACK)."""
        try:
            # Ждем ACK в течение 5 секунд
            ack = await asyncio.wait_for(self.ack_queue.get(), timeout=5)
            return ack == 'ack'
        except asyncio.TimeoutError:
            return False

    async def send_data_with_ack(self, type, content):
        """Отправка данных с подтверждением."""
        max_retries = 3
        retry_delay = 2  # Задержка между попытками (в секундах)

        for attempt in range(max_retries):
            try:
                await self.send_data(type, content)

                # Ждем ACK через обработчик сообщений
                ack_received = await self.wait_for_ack()
                if ack_received:
                    self.get_logger().info("Data delivered successfully.")
                    return True
                else:
                    self.get_logger().warning(f"Attempt {attempt + 1}: No ACK received. Retrying...")
                    await asyncio.sleep(retry_delay)
            except Exception as e:
                self.get_logger().error(f"Error sending data: {e}")
                return False

        self.get_logger().error("Failed to deliver data after multiple attempts.")
        return False

    async def send_data(self, type, content):
        """Отправка данных"""
        result_data = {
            'type': type,
            'content': content
        }

        try:
            # Проверка и обновление Access Token
            if not self.access_token:
                self.get_logger().error("No access token available.")
                return

            data_json = json.dumps(result_data, separators=(',', ':'))
            encrypted_data = encrypt_data(data_json)  # Шифруем данные

            # data_base64 = base64.b64encode(data_json.encode()).decode()

            signature = sign_message(encrypted_data)  

            payload = {
                'data': encrypted_data,
                'signature': signature
            }
            payload_str = json.dumps(payload)

            await self.websocket.send(payload_str)

            self.get_logger().info(f"Schemas sent successfully.")
        except Exception as e:
            self.get_logger().error(f"Error sending schemas: {e}")

    async def handle_websocket(self):
        """Обработка WebSocket соединения."""
        while True:
            try:
                # Проверяем актуальность токенов перед подключением
                if not self.ensure_tokens_are_valid():
                    self.get_logger().error("Failed to ensure tokens are valid. Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    continue

                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                }

                async with websockets.connect(self.websocket_url, additional_headers=headers) as websocket:
                    self.websocket = websocket
                    self.get_logger().info("WebSocket connection established.")

                    async for message in websocket:
                        data = json.loads(message)
                        if data.get('type') == 'ack':
                            await self.ack_queue.put('ack')  # Сохраняем ACK в очередь
                        elif data.get('type') == 'request_schemas':
                            self.get_logger().info("Received request for schemas")
                            schemas = self.get_all_schemas()
                            await self.send_data('schemas',schemas)
                        elif data.get('type') == 'subscribe':
                            topic_name = data.get('topic')
                            self.subscribe(topic_name)
                        elif data.get('type') == 'unsubscribe':
                            topic_name = data.get('topic')
                            self.unsubscribe(topic_name)
                        elif data.get('type') == 'refresh_token':
                            self.refresh_access_token()
            except Exception as e:
                self.get_logger().error(f"WebSocket error: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    def subscribe(self, topic_name):
        """Подписка на топик."""
        if topic_name in [sub.topic_name for sub in self.subscriptions]:
            self.get_logger().warning(f"Already subscribed to {topic_name}")
            return

        package = importlib.import_module('px4_msgs.msg')
      
        try:
            if not self.is_topic_active(topic_name):
                self.get_logger().error(f"Not an active topic {topic_name}: {e}")
                return

            name = topic_to_camel(topic_name)
            msg_type = getattr(package, name)
        
            # Создаем подписку
            self.create_subscription(
                msg_type,
                topic_name,
                lambda msg, tn=topic_name: self.generic_callback(msg, tn), 
                self.qos_profile
            )
            self.get_logger().info(f"Subscribed as {topic_name}")

        except AttributeError as e:
            self.get_logger().error(f"Failed to find message type {topic_name}: {e}")
            return  

    def unsubscribe(self, topic_name):
        """Отписка от топика."""
        if not topic_name:
            self.get_logger().error(f"Topic name not found for {topic_name}")
            return

        # Ищем подписку по имени топика
        for sub in self.subscriptions:
            if sub.topic_name == topic_name:
                # Уничтожаем подписку
                self.destroy_subscription(sub)
                self.get_logger().info(f"Unsubscribed as {topic_name}")
                return
        self.get_logger().warning(f"No subscription found for {topic_name}")

    def generic_callback(self, msg, topic_name):
        """Универсальный обработчик для всех топиков."""
        self.get_logger().info(f"Callback triggered for {topic_name}")
        try:
            data = {
                'name': msg.__class__.__name__,
                'topic': topic_name,
                'timestamp': msg.timestamp if hasattr(msg, 'timestamp') else 0,
                'data': self.extract_data(msg)
            }
            self.get_logger().info(f"Extracted data: {data}")
            
            # Добавляем данные в очередь
            self.message_queue.append(('data', data))
            
            # Обрабатываем очередь
            asyncio.create_task(self.process_queue())
        except Exception as e:
            self.get_logger().error(f"Error processing {topic_name}: {e}")

    def get_topic_name(self, name):
        """
        Проверяет и возвращает корректное имя топика.
        Если топик не найден с /fmu/out/, проверяет, существует ли он с /fmu/in/.
        """
        topic = f'/fmu/out/{camel_to_snake_case(name)}'
        active_topics = self.get_topic_names_and_types()

        for active_topic, _ in active_topics:
            if active_topic == topic:
                return topic 

        if topic.startswith('/fmu/out/'):
            fixed_topic_name = topic.replace('/fmu/out/', '/fmu/in/')
            for active_topic, _ in active_topics:
                if active_topic == fixed_topic_name:
                    return fixed_topic_name 
        return None

    def is_topic_active(self, topic_name):
        """Проверяет, активен ли топик."""
        try:
            active_topics = self.get_topic_names_and_types()
            for name, _ in active_topics:
                if name == topic_name:
                    return True
            return False
        except Exception as e:
            return False

    def extract_data(self, msg):
        """Универсальный метод для извлечения данных из сообщения PX4."""
        data = {}

        if not hasattr(msg, '__slots__'):
            return data
        
        internal_fields = {'_header', '_stamp'}  # Добавьте сюда другие служебные поля, если необходимо

        for field in msg.__slots__:
            if field in internal_fields:
                continue

            field_name = ''.join(word.capitalize() for word in field.lstrip('_').split('_'))
            value = getattr(msg, field)

            if isinstance(value, (int, float, np.floating)):
                if math.isnan(value):
                    data[field_name] = 'Nan'
                elif math.isinf(value):
                    data[field_name] = "Infinity" if value > 0 else "-Infinity"
                else:
                    data[field_name] = value
            elif isinstance(value, (list, tuple, np.ndarray)):
                if len(value) == 0:
                    data[field_name] = 'None'
                elif isinstance(value[0], (int, float, np.floating)):
                    data[field_name] = [float(v) for v in value]
                elif isinstance(value[0], str):
                    data[field_name] = list(value)
                else:
                    data[field_name] = [self.extract_data(v) for v in value] 
            elif hasattr(value, '__slots__'):  
                data[field_name] = self.extract_data(value)       
            else:
                data[field_name] = str(value)

        return data        

async def spin_node(node):
    """Асинхронный spin для ROS 2 узла."""
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.1)
        await asyncio.sleep(0.1)

def main(args=None):
    rclpy.init(args=args)
    px4_websocket_bridge = PX4WebSocketBridge()

    # Запуск асинхронного цикла событий
    loop = asyncio.get_event_loop()
    loop.create_task(spin_node(px4_websocket_bridge))
    loop.run_until_complete(px4_websocket_bridge.handle_websocket())

    # Остановка цикла событий
    loop.close()
    px4_websocket_bridge.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()

