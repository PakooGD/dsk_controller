import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy
import asyncio
import json
import importlib
from collections import deque
from src.services.websocket_service import WebSocketService
from src.services.auth_service import AuthHandler
from src.utils.helpers import get_local_ip, topic_to_camel, extract_data

WEBSOCKET_URL = "ws://localhost:8082"

class PX4WebSocketBridge(Node):
    def __init__(self):
        super().__init__('px4_websocket_bridge')
        
        # Configuration
        self.qos_profile = QoSProfile(depth=10)
        self.qos_profile.reliability = ReliabilityPolicy.BEST_EFFORT
        
        # Services
        self.auth = AuthHandler()
        self.websocket_service = WebSocketService(url=WEBSOCKET_URL)
        
        # Register handlers
        self.websocket_service.register_message_handler('subscribe', self._handle_subscribe)
        self.websocket_service.register_message_handler('unsubscribe', self._handle_unsubscribe)
        self.websocket_service.register_message_handler('fetchInfo', self._handle_fetch_info)

    async def run(self):
        """Main run loop"""
        while rclpy.ok():
            try:
                access_token = self.auth.load_tokens()
                if not self.auth.ensure_tokens_are_valid(access_token):
                    self.get_logger().error("Invalid tokens, cannot connect")
                    await asyncio.sleep(5)
                    continue

                await self.websocket_service.connect(headers={'Authorization': f'Bearer {access_token}'})
                
            except Exception as e:
                self.get_logger().error(f"Connection error: {e}")
                await asyncio.sleep(5)

    async def _handle_subscribe(self, data: dict):
        """Handle subscribe request"""
        topic_name = data.get('topic')
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
                lambda msg, tn=topic_name: self._topic_callback(msg, tn), 
                self.qos_profile
            )
            self.get_logger().info(f"Subscribed to {topic_name}")

        except Exception as e:
            self.get_logger().error(f"Error subscribing to {topic_name}: {e}")

    def _topic_callback(self, msg, topic_name):
        """Callback for subscribed topics"""
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
            asyncio.create_task(self.websocket_service.send(data))
        except Exception as e:
            self.get_logger().error(f"Error processing {topic_name}: {e}")

    async def _handle_unsubscribe(self, data: dict):
        """Handle unsubscribe request"""
        topic_name = data.get('topic')
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

    async def _handle_fetch_info(self, data: dict):
        """Handle info request"""
        info_message = {
            'type': 'info',
            'topics': self._get_all_active_topics(),
            'ip': get_local_ip(),
        }
        await self.websocket_service.send(info_message)

    def _get_all_active_topics(self):
        """Get list of all active topics with subscription status"""
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
    """Async spin for ROS 2 node"""
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0.1)
        await asyncio.sleep(0.1)

def main(args=None):
    rclpy.init(args=args)
    node = PX4WebSocketBridge()
    
    try:
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(
            node.run(),
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