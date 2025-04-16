import pytest
from src.web_app import app
import json

@pytest.fixture
def client():
    """Create a test client for the Flask app"""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_index_route(client):
    """Test the main dashboard route"""
    response = client.get('/')
    assert response.status_code == 200
    assert b'SIERRA' in response.data

def test_transform_api_empty_request(client):
    """Test the transform API with an empty request"""
    response = client.post('/api/transform',
                         data=json.dumps({}),
                         content_type='application/json')
    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data

def test_transform_api_valid_request(client):
    """Test the transform API with a valid request"""
    request_data = {
        'request': 'show all customers'
    }
    response = client.post('/api/transform',
                         data=json.dumps(request_data),
                         content_type='application/json')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['status'] == 'processing'
    assert data['request'] == request_data['request']

def test_history_api(client):
    """Test the history API endpoint"""
    response = client.get('/api/history')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'history' in data
    assert isinstance(data['history'], list) 