"""Testes reais via HTTP + confirmação de uma mensagem no Redpanda.
Execute após docker compose up --build -d, na raiz do projeto:
python tests/integration.py
"""
import json
import os
import subprocess
import unittest
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE = os.environ.get('API_URL', 'http://localhost:8081')


def payload():
    return {
        'id_evento': str(uuid.uuid4()), 'id_veiculo': 'CAM-TESTE',
        'sequencia': 1, 'timestamp': datetime.now(timezone.utc).isoformat(),
        'latitude': -19.9191, 'longitude': -43.9386,
        'velocidade': 72.5, 'motor_ligado': True,
    }


def send(data, correlation=None):
    raw = data if isinstance(data, bytes) else json.dumps(data).encode()
    headers = {'Content-Type': 'application/json'}
    if correlation:
        headers['X-Correlation-ID'] = correlation
    request = Request(BASE + '/api/telemetry', data=raw, headers=headers, method='POST')
    try:
        with urlopen(request, timeout=20) as response:
            return response.status, json.load(response), response.headers
    except HTTPError as response:
        return response.code, json.load(response), response.headers


class IntegrationTests(unittest.TestCase):
    def test_valid_event_reaches_broker(self):
        event = payload()
        correlation = str(uuid.uuid4())
        status, body, headers = send(event, correlation)
        self.assertEqual(202, status)
        self.assertEqual(correlation, headers['X-Correlation-ID'])
        self.assertEqual(event['id_evento'], body['id_evento'])
        result = subprocess.run([
            'docker', 'compose', 'exec', '-T', 'redpanda', 'rpk',
            'topic', 'consume', 'frota.telemetria', '-p', str(body['particao']),
            '-o', str(body['offset']), '-n', '1', '--format', '%v',
        ], capture_output=True, text=True, check=True, timeout=30)
        stored = json.loads(result.stdout)
        self.assertEqual(event['id_evento'], stored['id_evento'])
        self.assertEqual(event['id_veiculo'], stored['id_veiculo'])

    def test_invalid_coordinate_is_rejected(self):
        event = payload()
        event['latitude'] = 999
        status, body, _ = send(event)
        self.assertEqual(400, status)
        self.assertEqual('rejeitado', body['status'])

    def test_broken_json_is_rejected(self):
        self.assertEqual(400, send(b'{broken')[0])

    def test_missing_numeric_field_is_rejected(self):
        event = payload()
        del event['latitude']
        self.assertEqual(400, send(event)[0])

    def test_zero_and_false_are_valid(self):
        event = payload()
        event.update(latitude=0, longitude=0, velocidade=0, motor_ligado=False)
        self.assertEqual(202, send(event)[0])

    def test_delayed_event_is_accepted(self):
        event = payload()
        event['timestamp'] = '2020-01-01T00:00:00Z'
        self.assertEqual(202, send(event)[0])

    def test_timestamp_without_timezone_is_rejected(self):
        event = payload()
        event['timestamp'] = '2026-10-06T12:00:00'
        self.assertEqual(400, send(event)[0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
