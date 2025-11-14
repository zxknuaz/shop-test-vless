import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
import aiohttp
from shop_bot.data_manager.speedtest_runner import net_probe_for_host


@pytest.fixture
def mock_tcp_connection():
    """Мокирует TCP соединение"""
    async def mock_connect(host, port):
        reader = AsyncMock()
        writer = MagicMock()
        writer.close = AsyncMock()
        writer.wait_closed = AsyncMock()
        return reader, writer
    return mock_connect


@pytest.mark.asyncio
async def test_net_probe_for_host_ssl_disabled_success(mock_tcp_connection):
    """Тест что SSL проверка отключена при успешном HTTP HEAD запросе"""
    host_row = {
        'host_url': 'https://example.com'
    }
    
    with patch('asyncio.open_connection', side_effect=mock_tcp_connection):
        with patch('aiohttp.ClientSession') as mock_session:
            # Мокируем успешный HTTP ответ
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value="")
            
            # Правильно настраиваем async context manager для response
            # session.head() возвращает async context manager, который в __aenter__ возвращает response
            mock_response_context_manager = MagicMock()
            mock_response_context_manager.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response_context_manager.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_instance = AsyncMock()
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session_instance.__aexit__ = AsyncMock(return_value=False)
            # head() должен возвращать context manager напрямую (не через async функцию!)
            mock_session_instance.head = MagicMock(return_value=mock_response_context_manager)
            mock_session.return_value = mock_session_instance
            
            # Проверяем что connector создан с ssl=False
            connector_captured = []
            
            original_TCPConnector = aiohttp.TCPConnector
            
            def capture_connector(*args, **kwargs):
                connector_captured.append(kwargs.get('ssl', True))
                return original_TCPConnector(*args, **kwargs)
            
            with patch('aiohttp.TCPConnector', side_effect=capture_connector):
                result = await net_probe_for_host(host_row)
            
            # Проверяем что SSL был отключен
            assert len(connector_captured) > 0, f"Connector должен быть создан, но было создано {len(connector_captured)}"
            assert connector_captured[0] is False, f"SSL должен быть отключен (ssl=False), но было {connector_captured[0]}"
            if not result['ok']:
                print(f"Function returned error: {result.get('error', 'No error message')}")
            assert result['ok'] is True, f"Функция должна вернуть ok=True, но вернула {result}"
            assert 'http_ms' in result
            assert result['http_ms'] is not None


@pytest.mark.asyncio
async def test_net_probe_for_host_ssl_disabled_fallback_to_get(mock_tcp_connection):
    """Тест что SSL проверка отключена при fallback на GET запрос"""
    host_row = {
        'host_url': 'https://example.com'
    }
    
    with patch('asyncio.open_connection', side_effect=mock_tcp_connection):
        with patch('aiohttp.ClientSession') as mock_session:
            # Мокируем ошибку при HEAD, успех при GET
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value="test content")
            
            mock_session_instance = AsyncMock()
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session_instance.__aexit__ = AsyncMock(return_value=None)
            
            # HEAD выбрасывает исключение, GET работает
            mock_response_get = AsyncMock()
            mock_response_get.status = 200
            mock_response_get.text = AsyncMock(return_value="test content")
            
            mock_response_get_context_manager = MagicMock()
            mock_response_get_context_manager.__aenter__ = AsyncMock(return_value=mock_response_get)
            mock_response_get_context_manager.__aexit__ = AsyncMock(return_value=False)
            
            async def mock_head(*args, **kwargs):
                raise Exception("HEAD not supported")
            
            mock_session_instance.head = AsyncMock(side_effect=mock_head)
            mock_session_instance.get = MagicMock(return_value=mock_response_get_context_manager)
            mock_session.return_value = mock_session_instance
            
            # Проверяем что connector создан с ssl=False
            connector_captured = []
            
            original_TCPConnector = aiohttp.TCPConnector
            
            def capture_connector(*args, **kwargs):
                connector_captured.append(kwargs.get('ssl', True))
                return original_TCPConnector(*args, **kwargs)
            
            with patch('aiohttp.TCPConnector', side_effect=capture_connector):
                result = await net_probe_for_host(host_row)
            
            # Проверяем что SSL был отключен в обоих случаях (HEAD и GET fallback)
            assert len(connector_captured) >= 2, "Должно быть создано минимум 2 connector'а (для HEAD и GET)"
            assert all(ssl is False for ssl in connector_captured), "Все connector'ы должны иметь ssl=False"
            assert result['ok'] is True
            assert 'http_ms' in result


@pytest.mark.asyncio
async def test_net_probe_for_host_invalid_url():
    """Тест обработки невалидного URL"""
    host_row = {
        'host_url': 'invalid-url'
    }
    
    result = await net_probe_for_host(host_row)
    
    assert result['ok'] is False
    assert 'error' in result
    assert 'Invalid host_url' in result['error']


@pytest.mark.asyncio
async def test_net_probe_for_host_tcp_connection_failed():
    """Тест обработки ошибки TCP соединения"""
    host_row = {
        'host_url': 'https://nonexistent-domain-12345.com'
    }
    
    async def mock_connect_fail(*args, **kwargs):
        raise asyncio.TimeoutError("Connection timeout")
    
    with patch('asyncio.open_connection', side_effect=mock_connect_fail):
        result = await net_probe_for_host(host_row)
    
    assert result['ok'] is False
    assert 'error' in result
    assert 'TCP connect failed' in result['error']
