"""
图灵云打码平台集成模块
用于识别朝阳系选字验证码
"""

import base64
import json
import logging
import re
from typing import Optional


class TulingCloudOCR:
    """图灵云打码平台API调用类"""
    
    TULINGCLOUD_API_URL = "http://www.tulingcloud.com/tuling/predict"
    
    def __init__(self, username: str, password: str, model_id: str):
        """
        初始化图灵云API
        
        参数:
            username: 图灵云账户名
            password: 图灵云账户密码
            model_id: 识别模型ID (8位数字，用于选字验证码识别)
        """
        self.username = username
        self.password = password
        self.model_id = model_id
    
    def recognize_textclick(self, img_data: bytes) -> Optional[dict]:
        """
        识别选字验证码
        
        参数:
            img_data: 图片二进制数据
            
        返回:
            识别结果字典，包含 'text' 和可能的 'coordinates'，失败返回None
            示例: {"text": "朝阳系", "coordinates": [{"x": 100, "y": 200}, ...]}
        """
        try:
            import requests
            
            # 将图片编码为base64
            b64_data = base64.b64encode(img_data).decode('utf-8')
            
            # 构建请求数据
            data = {
                "username": self.username,
                "password": self.password,
                "ID": self.model_id,
                "b64": b64_data,
                "version": "3.1.1"
            }
            
            # 发送请求
            response = requests.post(
                self.TULINGCLOUD_API_URL,
                json=data,
                timeout=30
            )
            
            result = response.json()
            logging.debug(f"TulingCloud API Response: {result}")
            
            # 检查识别是否成功
            # API返回格式：
            # {
            #   "code": 1,
            #   "message": "",
            #   "data": {
            #     "顺序1": {"\u6587\u5b57": "\u5206", "X\u5750\u6807\u503c": 54, "Y\u5750\u6807\u503c": 28},
            #     "顺序2": {"\u6587\u5b57": "\u6d41", "X\u5750\u6807\u503c": 260, "Y\u5750\u6807\u503c": 50}
            #   }
            # }
            
            if result.get("code") in [0, 1]:  # code 0 or 1 both mean success
                response_data = result.get("data", {})
                
                # 处理图灵云的一牡七哨的珛c中文字段名
                if isinstance(response_data, dict):
                    coordinates = []
                    recognized_chars = []
                    
                    def _sort_key(key: str):
                        match = re.search(r"(\d+)$", str(key))
                        return int(match.group(1)) if match else 10**9

                    for key in sorted(response_data.keys(), key=_sort_key):
                        item = response_data.get(key)
                        if not isinstance(item, dict):
                            continue

                        char = item.get("文\u5b57") or item.get("text", "")
                        x = item.get("X\u5750\u6807\u503c") or item.get("x", 0)
                        y = item.get("Y\u5750\u6807\u503c") or item.get("y", 0)
                        
                        if char:
                            coordinates.append({
                                "x": int(x),
                                "y": int(y),
                                "text": str(char),
                                "source_key": str(key),
                            })
                            recognized_chars.append(str(char))
                            logging.debug(f"Parsed '{char}' at ({x}, {y}) from {key}")
                    
                    if recognized_chars and coordinates:
                        recognized_text = "".join(recognized_chars)
                        logging.info(f"TulingCloud recognized text: {recognized_text}")
                        logging.info(f"Coordinates: {coordinates}")
                        return {
                            "text": recognized_text,
                            "coordinates": coordinates,
                            "raw_result": result,
                        }
                    else:
                        logging.warning("TulingCloud returned empty result")
                        return None
                else:
                    logging.warning(f"Unexpected response data format: {type(response_data)}")
                    return None
            else:
                msg = result.get("message") or result.get("msg", "Unknown error")
                code = result.get("code", -1)
                logging.warning(f"TulingCloud recognition failed (code: {code}): {msg}")
                return None
                
        except ImportError:
            logging.error("requests library not installed. Install with: pip install requests")
            return None
        except json.JSONDecodeError:
            logging.error("Failed to parse TulingCloud API response")
            return None
        except Exception as e:
            logging.error(f"TulingCloud recognition failed: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            return None
    
    @staticmethod
    def query_balance(username: str, password: str) -> Optional[float]:
        """
        查询账户余额
        
        参数:
            username: 图灵云账户名
            password: 图灵云账户密码
            
        返回:
            余额（元），失败返回None
        """
        try:
            import requests
            
            # 构建查询请求
            # 注意：此方法需要根据图灵云API文档调整
            # 这是推测的实现，需要验证
            data = {
                "username": username,
                "password": password,
                "action": "getBalance"
            }
            
            response = requests.post(
                "http://www.tulingcloud.com/tuling/user/balance",
                json=data,
                timeout=30
            )
            
            result = response.json()
            
            if result.get("code") == 0:
                balance = float(result.get("data", {}).get("balance", 0))
                logging.debug(f"TulingCloud balance query success: {balance}")
                return balance
            else:
                logging.warning(f"TulingCloud balance query failed: {result.get('msg')}")
                return None
                
        except Exception as e:
            logging.error(f"TulingCloud balance query error: {e}")
            return None
