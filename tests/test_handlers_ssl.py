import pytest
import json
from unittest.mock import patch, AsyncMock, MagicMock
from decimal import Decimal
from aiohttp import TCPConnector
from shop_bot.bot.handlers import (
    get_usdt_rub_rate,
    get_ton_usdt_rate,
    _create_heleket_payment_request,
    _yoomoney_find_payment
)


@pytest.mark.asyncio
async def test_get_usdt_rub_rate_ssl_disabled():
    """Тест что SSL проверка отключена в get_usdt_rub_rate"""
    mock_response_data = {
        "tether": {
            "rub": 95.5
        }
    }
    
    with patch('aiohttp.ClientSession') as mock_session:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)
        
        mock_session_instance = AsyncMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.get = AsyncMock(return_value=mock_response.__aenter__())
        mock_session.return_value = mock_session_instance
        
        # Проверяем что connector создан с ssl=False
        original_connector_init = TCPConnector.__init__
        ssl_value_captured = []
        
        def capture_ssl_init(self, *args, **kwargs):
            ssl_value_captured.append(kwargs.get('ssl', None))
            return original_connector_init(self, *args, **kwargs)
        
        with patch.object(TCPConnector, '__init__', side_effect=capture_ssl_init):
            result = await get_usdt_rub_rate()
        
        # Проверяем что SSL был отключен
        assert len(ssl_value_captured) > 0
        assert ssl_value_captured[0] is False, "SSL должен быть отключен (ssl=False)"
        assert result == Decimal('95.5')


@pytest.mark.asyncio
async def test_get_usdt_rub_rate_http_error():
    """Тест обработки HTTP ошибки в get_usdt_rub_rate"""
    with patch('aiohttp.ClientSession') as mock_session:
        mock_response = AsyncMock()
        mock_response.status = 500
        
        mock_session_instance = AsyncMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.get = AsyncMock(return_value=mock_response.__aenter__())
        mock_session.return_value = mock_session_instance
        
        result = await get_usdt_rub_rate()
        
        assert result is None


@pytest.mark.asyncio
async def test_get_ton_usdt_rate_ssl_disabled():
    """Тест что SSL проверка отключена в get_ton_usdt_rate"""
    mock_response_data = {
        "toncoin": {
            "usd": 2.5
        }
    }
    
    with patch('aiohttp.ClientSession') as mock_session:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)
        
        mock_session_instance = AsyncMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.get = AsyncMock(return_value=mock_response.__aenter__())
        mock_session.return_value = mock_session_instance
        
        # Проверяем что connector создан с ssl=False
        original_connector_init = TCPConnector.__init__
        ssl_value_captured = []
        
        def capture_ssl_init(self, *args, **kwargs):
            ssl_value_captured.append(kwargs.get('ssl', None))
            return original_connector_init(self, *args, **kwargs)
        
        with patch.object(TCPConnector, '__init__', side_effect=capture_ssl_init):
            result = await get_ton_usdt_rate()
        
        # Проверяем что SSL был отключен
        assert len(ssl_value_captured) > 0
        assert ssl_value_captured[0] is False, "SSL должен быть отключен (ssl=False)"
        assert result == Decimal('2.5')


@pytest.mark.asyncio
async def test_create_heleket_payment_request_ssl_disabled():
    """Тест что SSL проверка отключена в _create_heleket_payment_request"""
    mock_response_data = {
        "payment_url": "https://heleket.com/pay/123"
    }
    
    with patch('shop_bot.bot.handlers.get_setting') as mock_get_setting:
        mock_get_setting.side_effect = lambda key: {
            'heleket_merchant_id': 'test_merchant',
            'heleket_api_key': 'test_api_key',
            'domain': 'example.com',
            'telegram_bot_username': 'test_bot',
            'heleket_api_base': 'https://api.heleket.com'
        }.get(key)
        
        with patch('shop_bot.bot.handlers.TELEGRAM_BOT_USERNAME', 'test_bot', create=True):
            with patch('aiohttp.ClientSession') as mock_session:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.text = AsyncMock(return_value=json.dumps(mock_response_data))
                mock_response.json = AsyncMock(return_value=mock_response_data)
                
                mock_session_instance = AsyncMock()
                mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
                mock_session_instance.__aexit__ = AsyncMock(return_value=None)
                mock_session_instance.post = AsyncMock(return_value=mock_response.__aenter__())
                mock_session.return_value = mock_session_instance
                
                # Проверяем что connector создан с ssl=False
                original_connector_init = TCPConnector.__init__
                ssl_value_captured = []
                
                def capture_ssl_init(self, *args, **kwargs):
                    ssl_value_captured.append(kwargs.get('ssl', None))
                    return original_connector_init(self, *args, **kwargs)
                
                with patch.object(TCPConnector, '__init__', side_effect=capture_ssl_init):
                    result = await _create_heleket_payment_request(
                        user_id=123,
                        price=100.0,
                        months=1,
                        host_name='test_host',
                        state_data={'action': 'new'}
                    )
                
                # Проверяем что SSL был отключен
                assert len(ssl_value_captured) > 0
                assert ssl_value_captured[0] is False, "SSL должен быть отключен (ssl=False)"
                assert result == "https://heleket.com/pay/123"


@pytest.mark.asyncio
async def test_create_heleket_payment_request_missing_credentials():
    """Тест обработки отсутствия credentials в _create_heleket_payment_request"""
    with patch('shop_bot.bot.handlers.get_setting') as mock_get_setting:
        mock_get_setting.side_effect = lambda key: None
        
        result = await _create_heleket_payment_request(
            user_id=123,
            price=100.0,
            months=1,
            host_name='test_host',
            state_data={'action': 'new'}
        )
        
        assert result is None


@pytest.mark.asyncio
async def test_yoomoney_find_payment_ssl_disabled():
    """Тест что SSL проверка отключена в _yoomoney_find_payment"""
    mock_response_data = {
        "operations": [
            {
                "label": "test_label",
                "direction": "in",
                "status": "success",
                "amount": "100.50",
                "operation_id": "12345",
                "datetime": "2024-01-01T00:00:00"
            }
        ]
    }
    
    with patch('shop_bot.bot.handlers.get_setting') as mock_get_setting:
        mock_get_setting.side_effect = lambda key: {
            'yoomoney_api_token': 'test_token'
        }.get(key)
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value=json.dumps(mock_response_data))
            mock_response.json = AsyncMock(return_value=mock_response_data)
            
            mock_session_instance = AsyncMock()
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session_instance.__aexit__ = AsyncMock(return_value=None)
            mock_session_instance.post = AsyncMock(return_value=mock_response.__aenter__())
            mock_session.return_value = mock_session_instance
            
            # Проверяем что connector создан с ssl=False
            original_connector_init = TCPConnector.__init__
            ssl_value_captured = []
            
            def capture_ssl_init(self, *args, **kwargs):
                ssl_value_captured.append(kwargs.get('ssl', None))
                return original_connector_init(self, *args, **kwargs)
            
            with patch.object(TCPConnector, '__init__', side_effect=capture_ssl_init):
                result = await _yoomoney_find_payment("test_label")
            
            # Проверяем что SSL был отключен
            assert len(ssl_value_captured) > 0
            assert ssl_value_captured[0] is False, "SSL должен быть отключен (ssl=False)"
            assert result is not None
            assert result['operation_id'] == "12345"
            assert result['amount'] == 100.50


@pytest.mark.asyncio
async def test_yoomoney_find_payment_missing_token():
    """Тест обработки отсутствия токена в _yoomoney_find_payment"""
    with patch('shop_bot.bot.handlers.get_setting') as mock_get_setting:
        mock_get_setting.side_effect = lambda key: None
        
        result = await _yoomoney_find_payment("test_label")
        
        assert result is None


@pytest.mark.asyncio
async def test_yoomoney_find_payment_http_error():
    """Тест обработки HTTP ошибки в _yoomoney_find_payment"""
    with patch('shop_bot.bot.handlers.get_setting') as mock_get_setting:
        mock_get_setting.side_effect = lambda key: {
            'yoomoney_api_token': 'test_token'
        }.get(key)
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 500
            mock_response.text = AsyncMock(return_value="Internal Server Error")
            
            mock_session_instance = AsyncMock()
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session_instance.__aexit__ = AsyncMock(return_value=None)
            mock_session_instance.post = AsyncMock(return_value=mock_response.__aenter__())
            mock_session.return_value = mock_session_instance
            
            result = await _yoomoney_find_payment("test_label")
            
            assert result is None
