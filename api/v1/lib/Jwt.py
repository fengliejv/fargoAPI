import time

import jwt


def create_token(platform: str):
    
    token_dict = {
        'iat': time.time(),
        'platform': platform
    }
    """payload 中一些固定参数名称的意义, 同时可以在payload中自定义参数"""
    
    
    
    
    
    
    

    
    headers = {
        'alg': "HS256",  
    }

    """headers 中一些固定参数名称的意义"""
    
    
    
    
    
    
    
    
    

    
    jwt_token = jwt.encode(token_dict,  
                           "fargoinsight",  
                           algorithm="HS256",  
                           headers=headers  
                           )

    return jwt_token


def check_token(token):
    try:
        token_dict = jwt.decode(token, "fargoinsight", algorithms=['HS256'])
        if time.time() - token_dict["iat"] > 24 * 60 * 60 * 300:
            return False, "token expired"
        else:
            return True, token_dict
    except Exception as e:
        return False, "token invalid"
