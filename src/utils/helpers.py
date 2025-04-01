import socket
import numpy as np
import math

def topic_to_camel(topic: str) -> str:
    return ''.join(word.capitalize() for word in topic.split('/')[-1].split('_'))

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

def extract_data(msg):
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