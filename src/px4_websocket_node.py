import os
import glob
import time
import asyncio
import websockets
import json
import rclpy
from rclpy.node import Node
from queue import Queue
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

SECRET_KEY = 'dsksupplytech' 
SECRET_MESSAGE_KEY = b'kchar-long-secret-key-1234567890'

DRONE_ID = str(uuid.uuid4())

# Blocklist типов сообщений
UNSUPPORTED_MSG_TYPES = []

# Подпись сообщения
def sign_message(message: str) -> str:
    """Подписывает сообщение с использованием HMAC."""
    hash = hmac.new(SECRET_KEY.encode(), message.encode(), sha256).hexdigest()
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

def encrypt_data(data) -> str:
    """Шифрует данные (строки или байты) с использованием AES."""
    if isinstance(data, str):
        data = data.encode()
    cipher = AES.new(SECRET_MESSAGE_KEY, AES.MODE_CBC)
    ct_bytes = cipher.encrypt(pad(data, AES.block_size))
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

        self.data_queue = Queue()

        self.websocket_url = 'ws://localhost:8082'
        self.http_url = 'http://localhost:5000/api'

        self.qos_profile = QoSProfile(depth=10)
        self.qos_profile.reliability = ReliabilityPolicy.BEST_EFFORT

        self.access_token = None
        self.refresh_token = None

        self.last_sent_file = None
        self.base_log_dir = "/home/alexei/Projects/PX4_ecosystem/src/client/PX4-Autopilot/build/px4_sitl_default/rootfs/log"
        
        self.authenticate()

    def get_latest_log_dir(self, base_dir):
        """Найти последнюю папку с логами в директории."""
        list_of_dirs = glob.glob(os.path.join(base_dir, "*/"))
        if not list_of_dirs:
            return None
        date_dirs = [d for d in list_of_dirs if os.path.basename(os.path.normpath(d)).count('-') == 2]
        if not date_dirs:
            return None
        latest_dir = max(date_dirs, key=os.path.getmtime)
        return latest_dir

    def get_latest_ulog_file(self, log_dir):
        """Получить последний созданный ulog-файл."""
        list_of_files = glob.glob(os.path.join(log_dir, "*.ulg"))
        if not list_of_files:
            return None
        latest_file = max(list_of_files, key=os.path.getctime)
        return latest_file

    async def send_ulog_file(self, file_path):
        """Отправка ulog-файла по частям с гарантированной доставкой."""
        if not os.path.exists(file_path):
            return False

        try:
            with open(file_path, 'rb') as f:
                chunk_index = 0
                while chunk := f.read(1024 * 1024):  # Читаем по 1 МБ
                    # Кодируем чанк в base64
                    chunk_base64 = base64.b64encode(chunk).decode('utf-8')

                    # Формируем payload
                    payload = {
                        'type': 'file_chunk',
                        'droneId': DRONE_ID,
                        'filename': os.path.basename(file_path),
                        'chunk_index': chunk_index,
                        'chunk_data': chunk_base64,  # Отправляем данные в base64
                        'is_last_chunk': f.tell() == os.path.getsize(file_path)
                    }

                    await self.send_data_with_ack('file_chunk', payload)
                    chunk_index += 1
            self.get_logger().info(f"File {file_path} sent successfully.")
            return True
        except Exception as e:
            self.get_logger().error(f"Error sending file: {e}")
            return False
        
    async def monitor_and_send(self, interval=10):
        """Мониторинг директории с настраиваемым интервалом."""
        while True:
            try:
                if self.access_token and not is_token_expired(self.access_token):
                    latest_dir = self.get_latest_log_dir(self.base_log_dir)
                    if latest_dir:
                        latest_file = self.get_latest_ulog_file(latest_dir)
                        if latest_file and latest_file != self.last_sent_file:
                            if await self.send_ulog_file(latest_file):
                                self.last_sent_file = latest_file
                    else:
                        self.get_logger().info("No log directories found.")
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                self.get_logger().info("Monitor task cancelled.")
                raise
            except Exception as e:
                self.get_logger().error(f"Error in monitor_and_send: {e}")
                await asyncio.sleep(interval)  # Продолжаем после ошибки

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

    def authenticate(self,retry_delay=5):
        """Авторизация дрона на сервере с повторными попытками."""
        while True:
            try:
                response = requests.post(f"{self.http_url}/auth", json={'drone_id': DRONE_ID,})
                
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

    async def process_queue(self):
        """Обработка очереди сообщений."""
        while True:   
            try: 
                if self.message_queue:
                    type, content = self.message_queue.popleft()
                    await self.send_data_with_ack(type, content)
            except Exception as e:
                self.get_logger().error(f"Error processing data queue: {e}")
            await asyncio.sleep(0.1)  # Небольшая задержка для снижения нагрузки на процессор

    async def send_data_with_ack(self, type, content, max_retries=3):
        """Отправка данных с подтверждением."""
        for attempt in range(max_retries):
            try:
                await self.send_data(type, content)

                ack_received = await self.wait_for_ack()
                if ack_received:
                    return True
                else:
                    self.get_logger().warning(f"Attempt {attempt + 1}: No ACK received. Retrying...")
            except Exception as e:
                self.get_logger().error(f"Error sending data: {e}")
            await asyncio.sleep(2)  # Задержка между попытками
        self.get_logger().error("Failed to deliver data after multiple attempts.")
        return False

    async def wait_for_ack(self):
        """Ожидание подтверждения (ACK)."""
        try:
            ack = await asyncio.wait_for(self.ack_queue.get(), timeout=5)
            if ack.get('type') == 'ack': 
                return True
            return False
        except asyncio.TimeoutError:
            return False

    async def send_data(self, type, content):
        """Отправка данных."""
        try:
            if not self.access_token:
                self.get_logger().error("No access token available.")
                return
            
            # Сериализуем данные в JSON
            data_json = json.dumps({'type': type, 'content': content}, separators=(',', ':'))
            
            # Шифруем данные
            encrypted_data = encrypt_data(data_json)
            signature = sign_message(encrypted_data)

            payload = {
                'data': encrypted_data,
                'signature': signature
            }
            await self.websocket.send(json.dumps(payload))
        except Exception as e:
            self.get_logger().error(f"Error sending data: {e}")

    async def handle_websocket(self):
        """Обработка WebSocket с экспоненциальной задержкой."""
        retry_delay = 1
        while True:
            try:
                if not self.ensure_tokens_are_valid():
                    self.get_logger().error("Failed to ensure tokens are valid. Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    continue

                headers = {'Authorization': f'Bearer {self.access_token}'}
                async with websockets.connect(self.websocket_url, additional_headers=headers) as websocket:
                    self.websocket = websocket
                    self.get_logger().info("WebSocket connection established.")

                    # Запуск мониторинга директории в отдельной задаче
                    monitor_task = asyncio.create_task(self.monitor_and_send())

                    # Запуск обработки очереди сообщений
                    process_task = asyncio.create_task(self.process_queue())

                    async for message in websocket:
                        data = json.loads(message)
                        if data.get('type') == 'ack':
                            await self.ack_queue.put(data)  # Сохраняем весь объект ack в очередь
                        elif data.get('type') == 'request_schemas':
                            schemas = self.get_all_schemas()
                            await self.send_data('schemas', schemas)
                        elif data.get('type') == 'subscribe':
                            topic_name = data.get('topic')
                            self.subscribe(topic_name)
                        elif data.get('type') == 'unsubscribe':
                            topic_name = data.get('topic')
                            self.unsubscribe(topic_name)
                        elif data.get('type') == 'refresh_token':
                            self.refresh_access_token()

                    # Остановка задач при разрыве соединения
                    monitor_task.cancel()
                    process_task.cancel()
                    try:
                        await monitor_task
                        await process_task
                    except asyncio.CancelledError:
                        self.get_logger().info("Tasks cancelled.")
            except Exception as e:
                self.get_logger().error(f"WebSocket error: {e}. Reconnecting in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Экспоненциальная задержка, максимум 60 секунд
                
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
        # self.get_logger().info(f"Callback triggered for {topic_name}")
        try:
            data = {
                'name': msg.__class__.__name__,
                'topic': topic_name,
                'timestamp': msg.timestamp if hasattr(msg, 'timestamp') else 0,
                'data': self.extract_data(msg)
            }
            # self.get_logger().info(f"Extracted data: {data}")

            # Добавляем данные в очередь
            self.message_queue.append(('data', data))

        except Exception as e:
            self.get_logger().error(f"Error processing {topic_name}: {e}")

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

    # Остановка цикла событий
    loop.close()
    px4_websocket_bridge.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()


