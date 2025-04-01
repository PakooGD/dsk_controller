import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy
import asyncio
import websockets
import json
import importlib
from collections import deque
from src.services.crypto_service import CryptoHandler
from src.services.auth_service import AuthHandler
from src.utils.helpers import get_local_ip, topic_to_camel, extract_data

# ===== Configuration =====
WEBSOCKET_SERVER = "ws://localhost:8082"
HTTP_URL = 'http://localhost:5000/api'
# =========================

class PX4WebSocketBridge(Node):
    def __init__(self):
        super().__init__('px4_websocket_bridge')
        
        self.crypto = CryptoHandler()
        self.auth = AuthHandler()

        self.message_queue = deque()
        self.ack_queue = asyncio.Queue()

        self.websocket = None
        self.process_task = None

        self.qos_profile = QoSProfile(depth=10)
        self.qos_profile.reliability = ReliabilityPolicy.BEST_EFFORT

    async def handle_websocket(self, retry_delay=1):
        """Обработка WebSocket соединения"""
        while rclpy.ok():
            try:
                access_token = self.auth.load_tokens()
                if not self.auth.ensure_tokens_are_valid(access_token):
                    self.get_logger().error("Invalid tokens, cannot connect")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)
                    continue

                retry_delay = 1

                async with websockets.connect(
                    WEBSOCKET_SERVER,
                    additional_headers={'Authorization': f'Bearer {access_token}'},
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    self.websocket = websocket
                    self.get_logger().info("WebSocket connected, initializing session...")
                    
                    try:
                        await self._initialize_session()
                        self.process_task = asyncio.create_task(self._process_outgoing_messages())
                        async for message in websocket:
                                await self._handle_incoming_message(message)
                    except websockets.exceptions.ConnectionClosed as e:
                        self.get_logger().error(f"WebSocket connection closed: {e}")
                    except Exception as e:
                        self.get_logger().error(f"Unexpected error in WebSocket handler: {e}")
                    finally:
                        if self.process_task:
                            self.process_task.cancel()
                            try:
                                await self.process_task
                            except asyncio.CancelledError:
                                pass
                        self.websocket = None
                        self.get_logger().info("WebSocket connection cleaned up")    
            except Exception as e:
                self.get_logger().error(f"Connection error: {e}, retrying in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
    

    async def _initialize_session(self):
        """Инициализация безопасной сессии"""
        try:
            data = await self.websocket.recv()
            if json.loads(data).get('type') == 'keyExchange':
                public_key = json.loads(data).get('publicKey')
                self.crypto.set_server_key(public_key)
                session_init = {
                    'type': 'session_init',
                    **self.crypto.encrypt_session_key()
                }
                await self.websocket.send(json.dumps(session_init))
                self.get_logger().info("Session initialized successfully")
        except Exception as e:
            self.get_logger().error(f"Error initializing session: {e}")
            raise

    async def _handle_incoming_message(self, message: str):
        """Обработка входящих сообщений"""
        try:
            data = json.loads(message)
            if data.get('type') == 'ack':
                await self.ack_queue.put(data)
            elif data.get('type') == 'subscribe':
                self._subscribe(data.get('topic'))
            elif data.get('type') == 'unsubscribe':
                self._unsubscribe(data.get('topic'))
            elif data.get('type') == 'fetchInfo':
                info_message = {
                    'type': 'info',
                    'topics': self.get_all_active_topics(),
                    'ip': get_local_ip(),
                }
                self.message_queue.append(info_message)    
        except json.JSONDecodeError:
            self.get_logger().error(f"Failed to decode message: {message}")
        except Exception as e:
            self.get_logger().error(f"Error processing message: {e}")

    async def _process_outgoing_messages(self):
        """Обработка исходящих сообщений"""
        while rclpy.ok() and self.websocket:
            try:
                if self.message_queue:
                    data = self.message_queue.popleft()
                    encrypted = self.crypto.encrypt_payload(data)
                    await self._send_with_retry(encrypted)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                self.get_logger().info("Outgoing message processing cancelled")
                raise
            except Exception as e:
                self.get_logger().error(f"Error in outgoing message processing: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on errors

    async def _send_with_retry(self, data: str, max_retries=3, base_delay=1):
        """Отправка данных с повторными попытками и экспоненциальной задержкой"""
        for attempt in range(max_retries):
            try:
                if not self.websocket:
                    self.get_logger().warning("WebSocket not connected, cannot send")
                    return False

                await self.websocket.send(data)
                try:
                    ack = await asyncio.wait_for(self.ack_queue.get(), timeout=5)
                    if ack.get('type') == 'ack':
                        return True
                    self.get_logger().warning(f"Invalid ACK received: {ack}")
                except asyncio.TimeoutError:
                    self.get_logger().warning(f"ACK timeout on attempt {attempt+1}")
                
            except websockets.exceptions.ConnectionClosed:
                self.get_logger().warning(f"Connection closed during send attempt {attempt+1}")
                return False
            except Exception as e:
                self.get_logger().error(f"Unexpected error in attempt {attempt+1}: {str(e)}")

            delay = min(base_delay * (2 ** attempt), 30)
            self.get_logger().info(f"Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
        
        self.get_logger().error(f"All {max_retries} attempts failed")
        return False

    def _subscribe(self, topic_name):
        """Подписка на топик."""
        try:
            if not topic_name:
                self.get_logger().error("No topic name provided for subscription")
                return

            if hasattr(self, 'subscriptions'):
                if topic_name in [sub.topic_name for sub in self.subscriptions]:
                    self.get_logger().info(f"Already subscribed to {topic_name}")
                    return

            package = importlib.import_module('px4_msgs.msg')
            name = topic_to_camel(topic_name)
            msg_type = getattr(package, name)
        
            self.create_subscription(
                msg_type,
                topic_name,
                lambda msg, tn=topic_name: self.generic_callback(msg, tn), 
                self.qos_profile
            )
            self.get_logger().info(f"Subscribed to {topic_name}")

        except ImportError:
            self.get_logger().error(f"Failed to import px4_msgs package")
        except AttributeError as e:
            self.get_logger().error(f"Failed to find message type {topic_name}: {e}")
        except Exception as e:
            self.get_logger().error(f"Error subscribing to {topic_name}: {e}") 

    def _unsubscribe(self, topic_name):
        """Отписка от топика."""
        try:
            if not topic_name:
                self.get_logger().error("No topic name provided for unsubscription")
                return

            if not hasattr(self, 'subscriptions'):
                self.get_logger().warning("No subscriptions exist")
                return

            for sub in self.subscriptions:
                if sub.topic_name == topic_name:
                    self.destroy_subscription(sub)
                    self.get_logger().info(f"Unsubscribed from {topic_name}")
                    return
            self.get_logger().warning(f"No subscription found for {topic_name}")
        except Exception as e:
            self.get_logger().error(f"Error unsubscribing from {topic_name}: {e}")
   
    def generic_callback(self, msg, topic_name):
        """Коллбэк-функция вызывается при подписке на топик и добавляет данные топика в очередь на отправку"""
        try:
            result = extract_data(msg)
            data = {
                'type': 'data',
                'name': msg.__class__.__name__,
                'topic': topic_name,
                'timestamp': msg.timestamp if hasattr(msg, 'timestamp') else 0,
                'schema': result['schema'],
                'data': result['data'],
            }
            self.message_queue.append(data)
        except Exception as e:
            self.get_logger().error(f"Error processing {topic_name}: {e}")

    def get_all_active_topics(self):
        """Получение списка активных топиков с их статусом подписки."""
        try:
            subscribed_topics = {sub.topic_name for sub in self.subscriptions} if hasattr(self, 'subscriptions') else set()
            
            return [
                {
                    'name': topic_to_camel(topic),
                    'topic': topic,
                    'status': topic in subscribed_topics
                }
                for topic, _ in self.get_topic_names_and_types()
            ]
        except Exception as e:
            self.get_logger().error(f"Error getting active topics: {e}")
            return []
  
async def spin_node(node):
    """Асинхронный spin для ROS 2 узла"""
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.1)
        await asyncio.sleep(0.1)

def main(args=None):
    rclpy.init(args=args)
    node = PX4WebSocketBridge()
    
    try:
        loop = asyncio.get_event_loop()
        
        tasks = asyncio.gather(
            node.handle_websocket(),
            spin_node(node)
        )
        
        loop.run_until_complete(tasks)
    except KeyboardInterrupt:
        node.get_logger().info("Shutting down by user request...")
    except Exception as e:
        node.get_logger().error(f"Unexpected error: {e}")
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()